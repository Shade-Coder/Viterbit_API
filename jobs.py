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
        
        self.log_file = os.path.join('logs', f'Jobs_execution_log_{self.timestamp}.txt')
        self.original_stdout = sys.stdout
        self.captured_output = StringIO()
        # Crear un objeto que escriba tanto en el buffer como en la consola original
        self.tee = TeeOutput(self.original_stdout, self.captured_output)
        sys.stdout = self.tee

    def _cleanup_old_logs(self):
        """Limpia los archivos de log antiguos.
        
        Esta función:
        1. Busca todos los archivos de log en la carpeta 'logs'
        2. Elimina los archivos que coincidan con el patrón 'Jobs_execution_log_*.txt'
        3. Maneja errores durante la eliminación de archivos
        """
        try:
            log_dir = 'logs'
            if os.path.exists(log_dir):
                for file in os.listdir(log_dir):
                    if file.startswith('Jobs_execution_log_') and file.endswith('.txt'):
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
JOBS_API_URL = f"{VITERBIT_BASE_URL}/jobs"
API_KEY = os.getenv('VITERBIT_API_KEY')
HEADERS = {"x-api-key": API_KEY}

# Configuración de API
MAX_RETRIES = 5
RETRY_DELAY = 2
PAGE_SIZE = 50
CONCURRENT_REQUESTS = 10
BATCH_SIZE = 50

# Configuración de Base de Datos
SQL_SERVER = os.getenv('SQL_SERVER', 'localhost')
SQL_DATABASE = os.getenv('SQL_DATABASE', 'ViterbitDB')

# Configuración de timeout y conexiones
TIMEOUT = ClientTimeout(total=60)
MAX_CONNECTIONS = 50

def get_db_connection():
    """Establece una conexión con la base de datos SQL Server.
    
    Returns:
        pyodbc.Connection: Objeto de conexión a la base de datos si es exitoso, None si falla.
    """
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

def create_jobs_table():
    """Crea la tabla Jobs en la base de datos si no existe.
    
    Esta función:
    1. Crea la tabla Jobs con todos los campos necesarios si no existe
    2. Verifica y añade columnas faltantes si la tabla ya existe
    3. Maneja errores durante la creación/modificación de la tabla
    
    La tabla incluye campos para:
    - Información básica del trabajo (id, título, descripción)
    - Detalles de ubicación y departamento
    - Información salarial y requisitos
    - Campos personalizados y metadatos
    """
    conn = get_db_connection()
    if not conn:
        return
    
    cursor = conn.cursor()
    try:
        # Primero crear la tabla si no existe
        cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Jobs' AND xtype='U')
        CREATE TABLE Jobs (
            id VARCHAR(50) PRIMARY KEY,
            reference VARCHAR(100),
            status VARCHAR(50),
            title NVARCHAR(255),
            slug NVARCHAR(255),
            slots INT,
            work_modality VARCHAR(50),
            created_at DATETIME,
            description NVARCHAR(MAX),
            requirements NVARCHAR(MAX),
            updated_at DATETIME,
            created_by_id VARCHAR(50),
            published_at DATETIME,
            department_id VARCHAR(50),
            department_profile_id VARCHAR(50),
            location_id VARCHAR(50),
            salary_periodicity VARCHAR(50),
            salary_min_amount DECIMAL(18,2),
            salary_min_currency VARCHAR(10),
            salary_max_amount DECIMAL(18,2),
            salary_max_currency VARCHAR(10),
            profile NVARCHAR(255),
            profile_area NVARCHAR(255),
            skills NVARCHAR(MAX),
            experience_years INT,
            owners_ids NVARCHAR(MAX),
            applies_until DATETIME,
            hiring_plan_requisition_id VARCHAR(100),
            contract_type_id VARCHAR(50),
            occupation VARCHAR(50),
            external_id VARCHAR(100),
            archived_at DATETIME
        )
        """)
        
        # Verificar si la tabla existe y añadir columnas faltantes
        cursor.execute("""
        IF EXISTS (SELECT * FROM sysobjects WHERE name='Jobs' AND xtype='U')
        BEGIN
            IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('Jobs') AND name = 'slug')
                ALTER TABLE Jobs ADD slug NVARCHAR(255);
            IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('Jobs') AND name = 'created_by_id')
                ALTER TABLE Jobs ADD created_by_id VARCHAR(50);
            IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('Jobs') AND name = 'department_id')
                ALTER TABLE Jobs ADD department_id VARCHAR(50);
            IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('Jobs') AND name = 'department_profile_id')
                ALTER TABLE Jobs ADD department_profile_id VARCHAR(50);
            IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('Jobs') AND name = 'salary_min_amount')
                ALTER TABLE Jobs ADD salary_min_amount DECIMAL(18,2);
            IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('Jobs') AND name = 'salary_min_currency')
                ALTER TABLE Jobs ADD salary_min_currency VARCHAR(10);
            IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('Jobs') AND name = 'salary_max_amount')
                ALTER TABLE Jobs ADD salary_max_amount DECIMAL(18,2);
            IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('Jobs') AND name = 'salary_max_currency')
                ALTER TABLE Jobs ADD salary_max_currency VARCHAR(10);
            IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('Jobs') AND name = 'owners_ids')
                ALTER TABLE Jobs ADD owners_ids NVARCHAR(MAX);
            IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('Jobs') AND name = 'applies_until')
                ALTER TABLE Jobs ADD applies_until DATETIME;
        END
        """)
        
        conn.commit()
        log_message("Tabla Jobs creada o actualizada exitosamente")
    except Exception as e:
        log_message(f"Error al crear/actualizar la tabla: {e}")
    finally:
        cursor.close()
        conn.close()

def create_ultima_actualizacion_table():
    """Crea la tabla Ultima_Actualizacion si no existe.
    
    Esta tabla almacena el registro de la última vez que se ejecutó cada script,
    permitiendo realizar actualizaciones incrementales en futuras ejecuciones.
    """
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

def get_last_update():
    """Obtiene la fecha de la última actualización desde la base de datos.
    
    Returns:
        str: Fecha de la última actualización en formato 'YYYY-MM-DD HH:MM:SS' o None si no existe.
    """
    conn = get_db_connection()
    if not conn:
        log_message("❌ No se pudo conectar a la base de datos para obtener última actualización")
        return None
    
    cursor = conn.cursor()
    try:
        # Primero verificar si existe la tabla
        cursor.execute("""
            SELECT COUNT(*) 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_NAME = 'Ultima_Actualizacion'
        """)
        table_exists = cursor.fetchone()[0] > 0

        if not table_exists:
            log_message("ℹ️ Tabla Ultima_Actualizacion no existe")
            return None

        # Obtener la última fecha de actualización para jobs.py
        cursor.execute("""
            SELECT ultima_actualizacion 
            FROM Ultima_Actualizacion 
            WHERE nombre_script = ?
        """, ('jobs.py',))
        
        result = cursor.fetchone()
        if result and result[0]:
            fecha_dt = result[0]
            # Restar un día a la fecha
            fecha_dt = fecha_dt - timedelta(days=1)
            log_message(f"📅 Última actualización encontrada (menos 1 día): {fecha_dt}")
            return fecha_dt.strftime("%Y-%m-%d %H:%M:%S")
        
        log_message("ℹ️ No se encontró registro de última actualización")
        return None
    except Exception as e:
        log_message(f"❌ Error al obtener última actualización: {e}")
        return None
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
        """, (fecha_actual, 'jobs.py'))
        
        # Si no se actualizó ninguna fila, insertar
        if cursor.rowcount == 0:
            cursor.execute("""
            INSERT INTO Ultima_Actualizacion (nombre_script, ultima_actualizacion)
            VALUES (?, ?)
            """, ('jobs.py', fecha_actual))
        
        conn.commit()
        log_message(f"✅ Actualizada última ejecución: {fecha_actual.strftime('%Y-%m-%d %H:%M:%S')}")
    except Exception as e:
        log_message(f"❌ Error al actualizar última ejecución: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

async def get_job_details_async(session: aiohttp.ClientSession, job_id: str) -> Dict[str, Any]:
    """Obtiene los detalles completos de un trabajo de forma asíncrona.
    
    Args:
        session (aiohttp.ClientSession): Sesión HTTP para realizar las peticiones.
        job_id (str): ID del trabajo a obtener.
    
    Returns:
        Dict[str, Any]: Datos del trabajo o None si hay error.
    """
    url = f"{JOBS_API_URL}/{job_id}?includes[]=custom_fields"
    retry_count = 0
    last_error = None
    
    while retry_count < MAX_RETRIES:
        try:
            async with session.get(url, headers=HEADERS) as response:
                if response.status == 200:
                    data = await response.json()
                    job_data = data.get('data', {})
                    if not job_data:
                        log_message(f"Trabajo {job_id} no tiene datos en la respuesta")
                        return None
                    return job_data
                elif response.status == 404:
                    log_message(f"Trabajo {job_id} no encontrado (404)")
                    return None
                elif response.status == 429:  # Rate limit
                    retry_after = int(response.headers.get('Retry-After', RETRY_DELAY))
                    log_message(f"Rate limit alcanzado para trabajo {job_id}, esperando {retry_after} segundos (Intento {retry_count + 1}/{MAX_RETRIES})")
                    await asyncio.sleep(retry_after)
                    retry_count += 1
                    continue
                elif response.status == 401:
                    log_message(f"Error de autenticación (401) para trabajo {job_id}. Verificar API key")
                    return None
                elif response.status == 403:
                    log_message(f"Error de autorización (403) para trabajo {job_id}. Sin permisos para acceder")
                    return None
                else:
                    error_text = await response.text()
                    log_message(f"Error {response.status} al obtener trabajo {job_id}")
                    log_message(f"URL: {url}")
                    log_message(f"Headers de respuesta: {dict(response.headers)}")
                    log_message(f"Respuesta: {error_text}")
                    last_error = f"Status {response.status}: {error_text}"
                    
                    if retry_count < MAX_RETRIES - 1:
                        await asyncio.sleep(RETRY_DELAY * (retry_count + 1))  # Exponential backoff
                        retry_count += 1
                        continue
                    return None
                    
        except aiohttp.ClientError as e:
            log_message(f"Error de conexión al obtener trabajo {job_id}: {str(e)}")
            last_error = str(e)
            if retry_count < MAX_RETRIES - 1:
                await asyncio.sleep(RETRY_DELAY * (retry_count + 1))
                retry_count += 1
                continue
        except asyncio.TimeoutError:
            last_error = "Timeout"
            if retry_count < MAX_RETRIES - 1:
                await asyncio.sleep(RETRY_DELAY * (retry_count + 1))
                retry_count += 1
                continue
        except Exception as e:
            log_message(f"Error inesperado al obtener trabajo {job_id}: {str(e)}")
            last_error = str(e)
            if retry_count < MAX_RETRIES - 1:
                await asyncio.sleep(RETRY_DELAY * (retry_count + 1))
                retry_count += 1
                continue
            return None
    
    if last_error:
        log_message(f"Agotados todos los reintentos para trabajo {job_id}. Último error: {last_error}")
    return None

async def process_job_batch(session: aiohttp.ClientSession, jobs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Procesa un lote de trabajos de forma asíncrona.
    
    Esta función:
    1. Divide el lote en sub-lotes más pequeños
    2. Procesa los trabajos en paralelo
    3. Maneja errores y reintentos para cada trabajo
    
    Args:
        session (aiohttp.ClientSession): Sesión HTTP para realizar las peticiones.
        jobs (List[Dict[str, Any]]): Lista de trabajos a procesar.
    
    Returns:
        List[Dict[str, Any]]: Lista de trabajos procesados exitosamente.
    """
    tasks = [get_job_details_async(session, job['id']) for job in jobs]
    return await asyncio.gather(*tasks)

async def get_page_async(session: aiohttp.ClientSession, page_number: int, date_param: str = None, date_value: str = None) -> tuple:
    """Obtiene una página de trabajos de forma asíncrona.
    
    Args:
        session (aiohttp.ClientSession): Sesión HTTP para realizar las peticiones.
        page_number (int): Número de página a obtener.
        date_param (str, optional): Parámetro de fecha para filtrar ('created_after' o 'updated_after').
        date_value (str, optional): Valor de la fecha para filtrar.
    
    Returns:
        tuple: (lista de trabajos, indicador de si hay más páginas)
    """
    base_url = f"{JOBS_API_URL}?page_size={PAGE_SIZE}&page={page_number}"
    if date_param and date_value:
        base_url += f"&{date_param}={date_value}"
    
    for attempt in range(MAX_RETRIES):
        try:
            async with session.get(base_url, headers=HEADERS) as response:
                if response.status == 200:
                    data = await response.json()
                    jobs = data.get("data", [])
                    meta = data.get("meta", {})
                    
                    # Log detallado de la página actual
                    log_message(f"📡 Página {page_number}: Obtenidos {len(jobs)} trabajos")
                    
                    # Procesar trabajos en lotes más grandes
                    detailed_jobs = []
                    for i in range(0, len(jobs), BATCH_SIZE):
                        batch = jobs[i:i + BATCH_SIZE]
                        batch_results = await process_job_batch(session, batch)
                        valid_jobs = [job for job in batch_results if job is not None]
                        detailed_jobs.extend(valid_jobs)
                        if len(valid_jobs) < len(batch):
                            log_message(f"📡 Página {page_number}, Lote {i//BATCH_SIZE + 1}: {len(valid_jobs)}/{len(batch)} trabajos válidos")
                    
                    has_more = meta.get("has_more", False)
                    total_pages = meta.get("total_pages", 0)
                    total_count = meta.get("total", 0)
                    
                    if page_number == 1:
                        log_message(f"📡 Total de registros según API ({date_param if date_param else 'todos'}): {total_count}")
                        log_message(f"📡 Total de páginas según API ({date_param if date_param else 'todos'}): {total_pages}")
                    
                    return detailed_jobs, has_more, total_pages, total_count
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

def save_to_database(jobs: List[Dict[str, Any]]) -> None:
    """Guarda los trabajos en la base de datos.
    
    Esta función:
    1. Procesa cada trabajo para aplanar su estructura
    2. Inserta o actualiza los registros en la tabla Jobs
    3. Maneja errores y reintentos para cada registro
    4. Proporciona un resumen detallado del proceso
    
    Args:
        jobs (List[Dict[str, Any]]): Lista de trabajos a guardar.
    """
    conn = get_db_connection()
    if not conn:
        log_message("No se pudo conectar a la base de datos.")
        return
    
    cursor = conn.cursor()
    try:
        # Preparar datos en lotes
        flat_jobs = [flatten_job(job) for job in jobs if flatten_job(job)]
        if not flat_jobs:
            log_message("No hay trabajos para guardar")
            return
        
        # Procesar en lotes de 1000 registros
        batch_size = 1000
        total_batches = (len(flat_jobs) + batch_size - 1) // batch_size
        
        for i in range(0, len(flat_jobs), batch_size):
            batch = flat_jobs[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            
            # Obtener IDs de los trabajos en el lote
            batch_ids = [job['id'] for job in batch]
            
            # Verificar IDs existentes en la base de datos
            cursor.execute("""
                SELECT id FROM Jobs 
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
            MERGE INTO Jobs WITH (HOLDLOCK) AS target
            USING (VALUES ({placeholders})) AS source ({columns})
            ON target.id = source.id
            WHEN MATCHED THEN
                UPDATE SET {update_columns}
            WHEN NOT MATCHED THEN
                INSERT ({columns})
                VALUES ({placeholders});
            """
            
            # Ejecutar MERGE para cada trabajo en el lote
            for job in batch:
                try:
                    values = tuple(job.values())
                    cursor.execute(merge_sql, values + values)
                except Exception as e:
                    log_message(f"❌ Error al procesar trabajo {job.get('id', 'N/A')}: {str(e)}")
            
            try:
                conn.commit()
                log_message(f"💾 Guardados {len(batch)} registros en la base de datos (Lote {batch_num}/{total_batches})")
            except Exception as e:
                conn.rollback()
                log_message(f"❌ Error al guardar lote {batch_num}: {str(e)}")
                raise
        
        log_message(f"💾 Total de trabajos guardados: {len(flat_jobs)}")
    except Exception as e:
        log_message(f"Error al guardar en la base de datos: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

def flatten_job(job: Dict[str, Any]) -> Dict[str, Any]:
    """Aplana y formatea los datos de un trabajo para su almacenamiento en la base de datos.
    
    Esta función:
    1. Extrae y formatea campos anidados
    2. Convierte fechas a formato compatible con SQL Server
    3. Maneja campos opcionales y valores por defecto
    4. Procesa campos personalizados y arrays
    
    Args:
        job (Dict[str, Any]): Datos del trabajo a aplanar.
    
    Returns:
        Dict[str, Any]: Datos del trabajo aplanados y formateados.
    """
    try:
        # Función para formatear fechas
        def format_date(date_str):
            if not date_str:
                return None
            try:
                dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                return dt.strftime('%Y-%m-%d %H:%M:%S')
            except (ValueError, AttributeError):
                return None

        # Procesar skills - Asegurarse de que se manejen correctamente los strings
        skills = job.get('skills', [])
        if isinstance(skills, list):
            # Convertir cada skill a string y eliminar espacios en blanco
            skills_list = [str(skill).strip() for skill in skills if skill]
            # Unir los skills con comas, asegurándose de que no haya espacios extra
            skills_str = ', '.join(filter(None, skills_list))
        else:
            skills_str = ''

        # Procesar owners_ids
        owners_ids = job.get('owners_ids', [])
        if isinstance(owners_ids, list):
            owners_ids_str = ', '.join(str(owner_id) for owner_id in owners_ids)
        else:
            owners_ids_str = ''

        # Obtener hiring_plan_requisition_id - Asegurarse de que se obtenga el valor correcto
        hiring_plan_requisition_id = str(job.get('hiring_plan_requisition_id', '')).strip()

        # Crear diccionario base con los campos exactos del JSON
        flattened = {
            'id': str(job.get('id', '')),
            'reference': str(job.get('reference', '')),
            'status': str(job.get('status', '')),
            'title': str(job.get('title', '')),
            'slug': str(job.get('slug', '')),
            'slots': int(job.get('slots', 0)),
            'work_modality': str(job.get('work_modality', '')),
            'created_at': format_date(job.get('created_at')),
            'description': str(job.get('description', '')),
            'requirements': str(job.get('requirements', '')),
            'updated_at': format_date(job.get('updated_at')),
            'created_by_id': str(job.get('created_by_id', '')),
            'published_at': format_date(job.get('published_at')),
            'department_id': str(job.get('department_id', '')),
            'department_profile_id': str(job.get('department_profile_id', '')),
            'location_id': str(job.get('location_id', '')),
            'salary_periodicity': str(job.get('salary_periodicity', '')),
            'salary_min_amount': float(job.get('salary_min', {}).get('amount', 0)),
            'salary_min_currency': str(job.get('salary_min', {}).get('currency', '')),
            'salary_max_amount': float(job.get('salary_max', {}).get('amount', 0)),
            'salary_max_currency': str(job.get('salary_max', {}).get('currency', '')),
            'profile': str(job.get('profile', '')),
            'profile_area': str(job.get('profile_area', '')),
            'skills': skills_str,
            'experience_years': int(job.get('experience_years', 0)),
            'owners_ids': owners_ids_str,
            'applies_until': format_date(job.get('applies_until')),
            'hiring_plan_requisition_id': hiring_plan_requisition_id,
            'contract_type_id': str(job.get('contract_type_id', '')),
            'occupation': str(job.get('occupation', '')),
            'external_id': str(job.get('external_id', '')),
            'archived_at': format_date(job.get('archived_at'))
        }
        
        return flattened
    except Exception as e:
        log_message(f"Error al aplanar el trabajo: {e}")
        return None

async def get_all_jobs_async(last_update: str = None) -> List[Dict[str, Any]]:
    """Obtiene todos los trabajos de la API de forma asíncrona.
    
    Esta función:
    1. Obtiene trabajos creados y actualizados desde la última ejecución
    2. Maneja la paginación y los rate limits de la API
    3. Procesa los datos en paralelo para mejorar el rendimiento
    4. Retorna la lista completa de trabajos
    
    Args:
        last_update (str, optional): Fecha de la última actualización en formato ISO.
    
    Returns:
        List[Dict[str, Any]]: Lista de trabajos obtenidos.
    """
    all_jobs = []
    start_time = time.time()
    total_expected = 0
    
    # Configurar el cliente HTTP con límites de conexión y timeout
    connector = TCPConnector(limit=MAX_CONNECTIONS, force_close=True)
    async with aiohttp.ClientSession(connector=connector, timeout=TIMEOUT) as session:
        if last_update:
            # Realizar consultas separadas para cada tipo de fecha
            date_params = ['published_after', 'created_after', 'updated_after']
            for date_param in date_params:
                log_message(f"🔄 Obteniendo trabajos con {date_param}...")
                jobs, has_more, total_pages, total_count = await get_page_async(session, 1, date_param, last_update)
                
                if jobs:
                    all_jobs.extend(jobs)
                    total_expected += total_count
                    
                    # Procesar el resto de páginas en paralelo
                    if total_pages > 1:
                        tasks = []
                        for page_num in range(2, total_pages + 1):
                            tasks.append(get_page_async(session, page_num, date_param, last_update))
                        
                        results = await asyncio.gather(*tasks, return_exceptions=True)
                        
                        for i, result in enumerate(results, 2):
                            if isinstance(result, Exception):
                                log_message(f"📡 Error en página {i}: {str(result)}")
                                continue
                                
                            jobs, _, _, _ = result
                            if jobs:
                                all_jobs.extend(jobs)
                                elapsed_time = time.time() - start_time
                                log_message(f"⚙️ Procesados {len(all_jobs)}/{total_expected} trabajos (Tiempo: {elapsed_time:.2f}s)")
        else:
            # Obtener todos los trabajos sin filtro de fecha
            jobs, has_more, total_pages, total_count = await get_page_async(session, 1)
            if jobs:
                all_jobs.extend(jobs)
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
                        
                    jobs, _, _, _ = result
                    if jobs:
                        all_jobs.extend(jobs)
                        elapsed_time = time.time() - start_time
                        log_message(f"⚙️ Procesados {len(all_jobs)}/{total_expected} trabajos (Tiempo: {elapsed_time:.2f}s)")
    
    total_time = time.time() - start_time
    log_message(f"⚙️ Proceso completado en {total_time:.2f} segundos")
    log_message(f"*** Total de trabajos obtenidos: {len(all_jobs)} de {total_expected} esperados")
    return all_jobs

async def main():
    """Función principal que orquesta el proceso de obtención y guardado de trabajos.
    
    Esta función:
    1. Crea las tablas necesarias en la base de datos
    2. Obtiene la fecha de la última actualización
    3. Obtiene los trabajos de la API
    4. Guarda los trabajos en la base de datos
    5. Registra la ejecución exitosa
    """
    with LogCapture():
        start_time = time.time()
        log_message("🚀 Iniciando proceso de actualización de trabajos...")
        
        try:
            # Crear tablas si no existen
            create_jobs_table()
            create_ultima_actualizacion_table()
            
            # Obtener última actualización
            last_update = get_last_update()
            if last_update:
                log_message(f"📅 Última actualización: {last_update}")
                # Obtener trabajos actualizados desde la última fecha
                log_message("\n🔄 Obteniendo trabajos actualizados y creados...")
                all_jobs = await get_all_jobs_async(last_update)
            else:
                log_message("\n🔄 No se encontró fecha de última actualización. Obteniendo todos los trabajos...")
                # Obtener todos los trabajos
                all_jobs = await get_all_jobs_async()
            
            if all_jobs:
                log_message("💾 Guardando trabajos en la base de datos...")
                save_to_database(all_jobs)
                # Actualizar la última ejecución solo si se guardaron los datos correctamente
                update_last_execution()
                log_message("✅ Datos guardados correctamente en la base de datos")
            else:
                log_message("❌ No se encontraron trabajos para guardar")
            
            duration = time.time() - start_time
            log_message(f"✨ Proceso completado. Duración: {duration:.2f} segundos")
            
        except Exception as e:
            log_message(f"❌ Error en el proceso: {str(e)}")
            raise

if __name__ == "__main__":
    asyncio.run(main())