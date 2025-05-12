import requests
import json
import time
import asyncio
import aiohttp
from datetime import datetime, timedelta
import os
import pyodbc
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor
from aiohttp import TCPConnector, ClientTimeout
import asyncio
from typing import List, Dict, Any
import sys
from io import StringIO
import shutil

# Variables globales para agrupar logs
error_pages = set()
retry_pages = set()
wait_messages = set()
last_log_time = None
problematic_pages = set()

class TeeOutput:
    """Clase que permite escribir en múltiples streams de salida simultáneamente.
    
    Esta clase se utiliza para duplicar la salida entre la consola y el archivo de log,
    asegurando que los mensajes se muestren tanto en pantalla como se guarden en el archivo.
    
    Attributes:
        files: Lista de archivos o streams donde se escribirá la salida.
    """
    def __init__(self, *files):
        self.files = files

    def write(self, obj):
        for f in self.files:
            f.write(obj)
            f.flush()

    def flush(self):
        for f in self.files:
            f.flush()

class LogCapture:
    """Clase para capturar y gestionar los logs de ejecución del script.
    
    Esta clase maneja la creación, limpieza y mantenimiento de archivos de log,
    asegurando que solo se mantenga un número específico de logs recientes.
    
    Attributes:
        timestamp (str): Marca de tiempo para el archivo de log actual.
        log_file (str): Ruta del archivo de log actual.
        original_stdout: Referencia al stdout original del sistema.
        captured_output: Buffer para capturar la salida.
        tee: Objeto TeeOutput para duplicar la salida.
    """
    def __init__(self):
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        # Crear carpeta logs si no existe
        if not os.path.exists('logs'):
            os.makedirs('logs')
        
        # Limpiar logs antiguos
        self._cleanup_old_logs()
        
        self.log_file = os.path.join('logs', f'Users_execution_log_{self.timestamp}.txt')
        self.original_stdout = sys.stdout
        self.captured_output = StringIO()
        # Crear un objeto que escriba tanto en el buffer como en la consola original
        self.tee = TeeOutput(self.original_stdout, self.captured_output)
        sys.stdout = self.tee

    def _cleanup_old_logs(self):
        """Limpia los archivos de log antiguos.
        
        Esta función:
        1. Busca todos los archivos de log en la carpeta 'logs'
        2. Elimina los archivos que coincidan con el patrón 'Users_execution_log_*.txt'
        3. Maneja errores durante la eliminación de archivos
        """
        try:
            log_dir = 'logs'
            if os.path.exists(log_dir):
                for file in os.listdir(log_dir):
                    if file.startswith('Users_execution_log_') and file.endswith('.txt'):
                        file_path = os.path.join(log_dir, file)
                        try:
                            os.remove(file_path)
                            print(f"🗑️ Eliminado log antiguo: {file}")
                        except Exception as e:
                            print(f"⚠️ Error al eliminar {file}: {e}")
        except Exception as e:
            print(f"⚠️ Error al limpiar logs antiguos: {e}")

    def __enter__(self):
        # Escribir encabezado del log
        header = f"\n{'='*80}\n"
        header += f"Ejecución iniciada: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        header += f"{'='*80}\n\n"
        print(header)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Escribir pie del log
        footer = f"\n{'='*80}\n"
        footer += f"Ejecución finalizada: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        if exc_type:
            footer += f"Estado: Error - {str(exc_val)}\n"
        else:
            footer += "Estado: Completado exitosamente\n"
        footer += f"{'='*80}\n"
        print(footer)
        
        # Restaurar stdout original
        sys.stdout = self.original_stdout
        
        # Solo guardar el log si la ejecución fue exitosa
        if not exc_type:
            # Guardar logs en archivo
            with open(self.log_file, 'w', encoding='utf-8') as f:
                f.write(self.captured_output.getvalue())
        
        # Limpiar el buffer
        self.captured_output.close()

def log_message(message: str, force_immediate: bool = False) -> None:
    """Registra un mensaje en el log con timestamp y agrupación inteligente.
    
    Esta función maneja diferentes tipos de mensajes:
    - Errores: Se agrupan por página
    - Reintentos: Se agrupan por página
    - Esperas: Se agrupan por tiempo de espera
    - Otros mensajes: Se muestran inmediatamente
    
    Args:
        message (str): Mensaje a registrar
        force_immediate (bool): Si es True, fuerza la impresión inmediata sin agrupar
    """
    global error_pages, retry_pages, wait_messages, last_log_time
    current_time = datetime.now()
    timestamp = current_time.strftime("%Y-%m-%d %H:%M:%S")
    
    # Si es un mensaje de error o reintento, lo agrupamos
    if "Error en la página" in message:
        page = message.split("página")[-1].strip()
        error_pages.add(page)
        if force_immediate or (last_log_time and (current_time - last_log_time).seconds >= 2):
            log_entry = f"[{timestamp}] ❌ Error en las páginas: {', '.join(sorted(error_pages))}"
            print(log_entry)
            error_pages.clear()
            last_log_time = current_time
    elif "Intento" in message and "para la página" in message:
        page = message.split("página")[-1].strip()
        if page in error_pages:
            retry_pages.add(page)
            if force_immediate or (last_log_time and (current_time - last_log_time).seconds >= 2):
                log_entry = f"[{timestamp}] 🔄 Intento {message.split('Intento')[1].split('para')[0].strip()} para las páginas: {', '.join(sorted(retry_pages))}"
                print(log_entry)
                retry_pages.clear()
                last_log_time = current_time
    elif "Esperando" in message:
        wait_time = message.split("Esperando")[-1].strip()
        wait_messages.add(wait_time)
        if force_immediate or (last_log_time and (current_time - last_log_time).seconds >= 2):
            if len(wait_messages) > 1:
                log_entry = f"[{timestamp}] ⏳ Múltiples esperas activas: {', '.join(sorted(wait_messages))}"
            else:
                log_entry = f"[{timestamp}] ⏳ {next(iter(wait_messages))}"
            print(log_entry)
            wait_messages.clear()
            last_log_time = current_time
    else:
        log_entry = f"[{timestamp}] {message}"
        print(log_entry)
        last_log_time = current_time

# Cargar variables de entorno
load_dotenv()

# Configuración de la API
VITERBIT_BASE_URL = "https://api.viterbit.com/v1"
USERS_API_URL = f"{VITERBIT_BASE_URL}/users"
API_KEY = os.getenv('VITERBIT_API_KEY')
HEADERS = {"x-api-key": API_KEY}

# Configuración de archivos
ARCHIVO_USERS = "users.json"

# Configuración de API
MAX_RETRIES = 5
RETRY_DELAY = 2
PAGE_SIZE = 100
CONCURRENT_REQUESTS = 10
BATCH_SIZE = 50

# Configuración de Base de Datos
SQL_SERVER = os.getenv('SQL_SERVER', 'localhost')
SQL_DATABASE = os.getenv('SQL_DATABASE', 'ViterbitDB')

# Configuración de timeout y conexiones
TIMEOUT = ClientTimeout(total=60)
MAX_CONNECTIONS = 50

def get_db_connection():
    """Establece conexión con la base de datos SQL Server"""
    conn_str = (
        f'DRIVER={{ODBC Driver 17 for SQL Server}};'
        f'SERVER={SQL_SERVER};'
        f'DATABASE={SQL_DATABASE};'
        'Trusted_Connection=yes;'
    )
    try:
        return pyodbc.connect(conn_str)
    except Exception as e:
        log_message(f"Error al conectar con la base de datos: {e}")
        return None

def create_users_table():
    """Crea la tabla de usuarios si no existe"""
    conn = get_db_connection()
    if not conn:
        return
    
    cursor = conn.cursor()
    try:
        # Verificar si la tabla existe
        cursor.execute("""
            SELECT COUNT(*) 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_NAME = 'Users'
        """)
        table_exists = cursor.fetchone()[0] > 0

        if not table_exists:
            # Crear la tabla con la estructura necesaria
            cursor.execute("""
            CREATE TABLE Users (
                id VARCHAR(50) PRIMARY KEY,
                first_name NVARCHAR(255),
                last_name NVARCHAR(255),
                full_name NVARCHAR(255),
                email NVARCHAR(255),
                reference VARCHAR(255),
                phone NVARCHAR(255),
                business_role VARCHAR(50),
                picture_url NVARCHAR(MAX),
                last_login_at DATETIME,
                created_at DATETIME,
                updated_at DATETIME
            )
            """)
            conn.commit()
            log_message("✅ Tabla Users creada exitosamente")
    except Exception as e:
        log_message(f"❌ Error al crear la tabla Users: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

def create_ultima_actualizacion_table():
    """Crea la tabla Ultima_Actualizacion si no existe"""
    conn = get_db_connection()
    if not conn:
        return
    
    cursor = conn.cursor()
    try:
        # Verificar si la tabla existe
        cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Ultima_Actualizacion' AND xtype='U')
        BEGIN
            CREATE TABLE Ultima_Actualizacion (
                nombre_script VARCHAR(100) PRIMARY KEY,
                ultima_actualizacion DATETIME
            )
        END
        """)
        conn.commit()
        log_message("💾 Tabla Ultima_Actualizacion verificada exitosamente")
    except Exception as e:
        log_message(f"Error al verificar la tabla Ultima_Actualizacion: {e}")
    finally:
        cursor.close()
        conn.close()

def update_last_execution():
    """Registra la ejecución actual en la tabla Ultima_Actualizacion.
    
    Esta función actualiza o inserta el registro de la última ejecución exitosa,
    permitiendo realizar actualizaciones incrementales en futuras ejecuciones.
    """
    conn = get_db_connection()
    if not conn:
        log_message("❌ No se pudo conectar a la base de datos para actualizar última ejecución")
        return
    
    cursor = conn.cursor()
    try:
        fecha_actual = datetime.now()
        
        # Verificar si existe la tabla
        cursor.execute("""
            SELECT COUNT(*) 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_NAME = 'Ultima_Actualizacion'
        """)
        table_exists = cursor.fetchone()[0] > 0

        if not table_exists:
            # Crear la tabla si no existe
            cursor.execute("""
            CREATE TABLE Ultima_Actualizacion (
                nombre_script VARCHAR(100) PRIMARY KEY,
                ultima_actualizacion DATETIME
            )
            """)
            log_message("💾 Tabla Ultima_Actualizacion creada")
        
        # Intentar actualizar primero
        cursor.execute("""
        UPDATE Ultima_Actualizacion 
        SET ultima_actualizacion = ?
        WHERE nombre_script = ?
        """, (fecha_actual, 'users.py'))
        
        # Si no se actualizó ninguna fila, insertar
        if cursor.rowcount == 0:
            cursor.execute("""
            INSERT INTO Ultima_Actualizacion (nombre_script, ultima_actualizacion)
            VALUES (?, ?)
            """, ('users.py', fecha_actual))
        
        conn.commit()
        log_message(f"✅ Actualizada última ejecución: {fecha_actual.strftime('%Y-%m-%d %H:%M:%S')}")
    except Exception as e:
        log_message(f"❌ Error al actualizar última ejecución: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

def flatten_user(user: Dict[str, Any]) -> Dict[str, Any]:
    """Aplana el objeto de usuario para almacenarlo en la base de datos.
    
    Args:
        user (Dict[str, Any]): Objeto de usuario original
        
    Returns:
        Dict[str, Any]: Objeto de usuario aplanado
    """
    def format_date(date_str):
        if not date_str:
            return None
        try:
            return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        except (ValueError, AttributeError):
            return None

    return {
        'id': user.get('id'),
        'first_name': user.get('first_name'),
        'last_name': user.get('last_name'),
        'full_name': user.get('full_name'),
        'email': user.get('email'),
        'reference': user.get('reference'),
        'phone': user.get('phone'),
        'business_role': user.get('business_role'),
        'picture_url': user.get('picture_url'),
        'last_login_at': format_date(user.get('last_login_at')),
        'created_at': format_date(user.get('created_at')),
        'updated_at': format_date(user.get('updated_at'))
    }

def save_to_database(users: List[Dict[str, Any]]) -> None:
    """Guarda los usuarios en la base de datos.
    
    Args:
        users (List[Dict[str, Any]]): Lista de usuarios a guardar
    """
    if not users:
        log_message("⚠️ No hay usuarios para guardar")
        return

    conn = get_db_connection()
    if not conn:
        return

    cursor = conn.cursor()
    try:
        # Preparar datos en lotes
        flat_users = [flatten_user(user) for user in users if flatten_user(user)]
        if not flat_users:
            log_message("No hay usuarios para guardar")
            return
        
        # Procesar en lotes de 1000 registros
        batch_size = 1000
        total_batches = (len(flat_users) + batch_size - 1) // batch_size
        
        for i in range(0, len(flat_users), batch_size):
            batch = flat_users[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            
            # Obtener IDs de los usuarios en el lote
            batch_ids = [user['id'] for user in batch]
            
            # Verificar IDs existentes en la base de datos
            cursor.execute("""
                SELECT id FROM Users 
                WHERE id IN ({})
            """.format(','.join(['?'] * len(batch_ids))), batch_ids)
            
            existing_ids = {row[0] for row in cursor.fetchall()}
            new_ids = set(batch_ids) - existing_ids
            
            log_message(f"📊 Lote {batch_num}/{total_batches}:")
            log_message(f"   - IDs existentes: {len(existing_ids)}")
            log_message(f"   - IDs nuevos: {len(new_ids)}")
            
            # Construir la consulta MERGE
            columns = ', '.join(batch[0].keys())
            placeholders = ', '.join(['?' for _ in batch[0]])
            update_columns = ', '.join([f"target.{col} = source.{col}" for col in batch[0].keys() if col != 'id'])
            
            merge_sql = f"""
            MERGE INTO Users WITH (HOLDLOCK) AS target
            USING (VALUES ({placeholders})) AS source ({columns})
            ON target.id = source.id
            WHEN MATCHED THEN
                UPDATE SET {update_columns}
            WHEN NOT MATCHED THEN
                INSERT ({columns})
                VALUES ({placeholders});
            """
            
            # Ejecutar MERGE para cada usuario en el lote
            for user in batch:
                try:
                    values = tuple(user.values())
                    cursor.execute(merge_sql, values + values)
                except Exception as e:
                    log_message(f"❌ Error al procesar usuario {user.get('id', 'N/A')}: {str(e)}")
            
            try:
                conn.commit()
                log_message(f"💾 Guardados {len(batch)} registros en la base de datos (Lote {batch_num}/{total_batches})")
            except Exception as e:
                conn.rollback()
                log_message(f"❌ Error al guardar lote {batch_num}: {str(e)}")
                raise
        
        log_message(f"💾 Total de usuarios guardados: {len(flat_users)}")
    except Exception as e:
        log_message(f"Error al guardar en la base de datos: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

async def get_page_async(session: aiohttp.ClientSession, page_number: int) -> tuple:
    """Obtiene una página de usuarios de forma asíncrona.
    
    Args:
        session (aiohttp.ClientSession): Sesión HTTP para realizar las peticiones.
        page_number (int): Número de página a obtener.
    
    Returns:
        tuple: (lista de usuarios, indicador de si hay más páginas)
    """
    url = f"{USERS_API_URL}?page_size={PAGE_SIZE}&page={page_number}"
    
    for attempt in range(MAX_RETRIES):
        try:
            async with session.get(url, headers=HEADERS) as response:
                if response.status == 200:
                    data = await response.json()
                    users = data.get("data", [])
                    meta = data.get("meta", {})
                    
                    # Log detallado de la página actual
                    log_message(f"📡 Página {page_number}: Obtenidos {len(users)} usuarios")
                    
                    has_more = meta.get("has_more", False)
                    total_pages = meta.get("total_pages", 0)
                    total_count = meta.get("total", 0)
                    
                    if page_number == 1:
                        log_message(f"📡 Total de registros según API: {total_count}")
                        log_message(f"📡 Total de páginas según API: {total_pages}")
                    
                    return users, has_more, total_pages, total_count
                elif response.status == 429:  # Rate limit
                    retry_after = int(response.headers.get('Retry-After', RETRY_DELAY))
                    log_message(f"📡 Rate limit alcanzado en página {page_number}, esperando {retry_after} segundos")
                    await asyncio.sleep(retry_after)
                    continue
                else:
                    log_message(f"📡 Error {response.status} al obtener página {page_number}")
        except Exception as e:
            log_message(f"📡 Error al procesar página {page_number}: {str(e)}")
            if attempt < MAX_RETRIES - 1:
                await asyncio.sleep(RETRY_DELAY)
            continue
    return [], False, 0, 0

async def get_all_users_async() -> List[Dict[str, Any]]:
    """Obtiene todos los usuarios de la API de forma asíncrona.
    
    Esta función:
    1. Maneja la paginación y los rate limits de la API
    2. Procesa los datos en paralelo para mejorar el rendimiento
    3. Retorna la lista completa de usuarios
    
    Returns:
        List[Dict[str, Any]]: Lista de usuarios obtenidos.
    """
    all_users = []
    start_time = time.time()
    total_expected = 0
    
    # Configurar el cliente HTTP con límites de conexión y timeout
    connector = TCPConnector(limit=MAX_CONNECTIONS, force_close=True)
    async with aiohttp.ClientSession(connector=connector, timeout=TIMEOUT) as session:
        # Obtener todos los usuarios sin filtro de fecha
        users, has_more, total_pages, total_count = await get_page_async(session, 1)
        if users:
            all_users.extend(users)
            total_expected = total_count
            log_message(f"🔄 Inicio del proceso. . .")
            log_message(f"🔄 Total de páginas a procesar: {total_pages}")
        
        # Procesar el resto de páginas en paralelo
        if total_pages > 1:
            tasks = []
            for page_num in range(2, total_pages + 1):
                tasks.append(get_page_async(session, page_num))
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for i, result in enumerate(results, 2):
                if isinstance(result, Exception):
                    log_message(f"📡 Error en página {i}: {str(result)}")
                    continue
                    
                users, _, _, _ = result
                if users:
                    all_users.extend(users)
                    elapsed_time = time.time() - start_time
                    log_message(f"⚙️ Procesados {len(all_users)}/{total_expected} usuarios (Tiempo: {elapsed_time:.2f}s)")
    
    total_time = time.time() - start_time
    log_message(f"⚙️ Proceso completado en {total_time:.2f} segundos")
    log_message(f"*** Total de usuarios obtenidos: {len(all_users)} de {total_expected} esperados")
    return all_users

async def main():
    """Función principal que orquesta el proceso de obtención y guardado de usuarios.
    
    Esta función:
    1. Crea las tablas necesarias en la base de datos
    2. Obtiene los usuarios de la API
    3. Guarda los usuarios en la base de datos
    4. Registra la ejecución exitosa
    """
    with LogCapture():
        start_time = time.time()
        log_message("🚀 Iniciando proceso de actualización de usuarios...")
        
        try:
            # Crear tablas si no existen
            create_users_table()
            create_ultima_actualizacion_table()
            
            # Obtener todos los usuarios
            log_message("\n🔄 Obteniendo todos los usuarios...")
            all_users = await get_all_users_async()
            
            if all_users:
                log_message("💾 Guardando usuarios en la base de datos...")
                save_to_database(all_users)
                # Actualizar la última ejecución solo si se guardaron los datos correctamente
                update_last_execution()
                log_message("✅ Datos guardados correctamente en la base de datos")
            else:
                log_message("❌ No se encontraron usuarios para guardar")
            
            duration = time.time() - start_time
            log_message(f"✨ Proceso completado. Duración: {duration:.2f} segundos")
            
        except Exception as e:
            log_message(f"❌ Error en el proceso: {str(e)}")
            raise

if __name__ == "__main__":
    asyncio.run(main())