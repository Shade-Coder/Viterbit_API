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
from typing import List, Dict, Any, Optional, Tuple
import sys
from io import StringIO

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
        max_logs (int): Número máximo de archivos de log a mantener.
        log_file (str): Ruta del archivo de log actual.
        original_stdout: Referencia al stdout original del sistema.
        captured_output: Buffer para capturar la salida.
        tee: Objeto TeeOutput para duplicar la salida.
    """
    def __init__(self, max_logs=1):
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.max_logs = max_logs  # Número máximo de logs a mantener
        
        # Crear carpeta logs si no existe
        if not os.path.exists('logs'):
            os.makedirs('logs')
        
        # Limpiar logs antiguos
        self._cleanup_old_logs()
        
        self.log_file = os.path.join('logs', f'Candidatures_execution_log_{self.timestamp}.txt')
        self.original_stdout = sys.stdout
        self.captured_output = StringIO()
        # Crear un objeto que escriba tanto en el buffer como en la consola original
        self.tee = TeeOutput(self.original_stdout, self.captured_output)
        sys.stdout = self.tee

    def _cleanup_old_logs(self):
        """Limpia los archivos de log antiguos, manteniendo solo los más recientes.
        
        Esta función:
        1. Obtiene la lista de archivos de log existentes
        2. Los ordena por fecha de modificación
        3. Elimina los archivos más antiguos que excedan el límite establecido
        """
        try:
            # Obtener lista de archivos de log
            log_files = []
            for file in os.listdir('logs'):
                if file.startswith('Candidatures_execution_log_') and file.endswith('.txt'):
                    full_path = os.path.join('logs', file)
                    log_files.append((full_path, os.path.getmtime(full_path)))
            
            # Ordenar por fecha de modificación (más reciente primero)
            log_files.sort(key=lambda x: x[1], reverse=True)
            
            # Eliminar logs antiguos si exceden el máximo
            if len(log_files) >= self.max_logs:
                for file_path, _ in log_files[self.max_logs:]:
                    try:
                        os.remove(file_path)
                        log_message(f"🗑️ Eliminado log antiguo: {os.path.basename(file_path)}")
                    except Exception as e:
                        log_message(f"⚠️ Error al eliminar log antiguo {file_path}: {str(e)}")
        except Exception as e:
            log_message(f"⚠️ Error al limpiar logs antiguos: {str(e)}")

    def _get_last_log_content(self):
        """Obtiene el contenido del último archivo de log si existe.
        
        Returns:
            str: Contenido del último archivo de log, o None si no existe.
        """
        try:
            log_files = []
            for file in os.listdir('logs'):
                if file.startswith('Candidatures_execution_log_') and file.endswith('.txt'):
                    full_path = os.path.join('logs', file)
                    log_files.append((full_path, os.path.getmtime(full_path)))
            
            if log_files:
                # Ordenar por fecha de modificación (más reciente primero)
                log_files.sort(key=lambda x: x[1], reverse=True)
                last_log_path = log_files[0][0]
                
                with open(last_log_path, 'r', encoding='utf-8') as f:
                    return f.read()
        except Exception as e:
            log_message(f"⚠️ Error al leer último log: {str(e)}")
        return None

    def __enter__(self):
        # Escribir encabezado del log
        header = f"\n{'='*80}\n"
        header += f"Ejecución iniciada: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        
        # Agregar información del último log si existe
        last_log = self._get_last_log_content()
        if last_log:
            header += f"\nÚltima ejecución encontrada en: {os.path.basename(self.log_file)}\n"
        
        header += f"{'='*80}\n\n"
        print(header)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Restaurar stdout original
        sys.stdout = self.original_stdout
        
        # Escribir pie del log
        footer = f"\n{'='*80}\n"
        footer += f"Ejecución finalizada: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        if exc_type:
            footer += f"Estado: Error - {exc_type.__name__}: {str(exc_val)}\n"
        else:
            footer += "Estado: Completado exitosamente\n"
        footer += f"{'='*80}\n"
        print(footer)
        
        # Guardar el contenido del buffer en el archivo
        with open(self.log_file, 'w', encoding='utf-8') as f:
            f.write(self.captured_output.getvalue())
        
        # Cerrar el buffer
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
CANDIDATURES_API_URL = f"{VITERBIT_BASE_URL}/candidatures"
API_KEY = os.getenv('VITERBIT_API_KEY')
HEADERS = {"x-api-key": API_KEY}

# Configuración de API
MAX_RETRIES = 5
RETRY_DELAY = 2
PAGE_SIZE = 100
CONCURRENT_REQUESTS = 100
BATCH_SIZE = 1000  # Aumentado para optimizar rendimiento
LIMIT_RECORDS = 1000  # Límite de registros para pruebas. Si es 0, trae todos los registros

# Configuración de Base de Datos
SQL_SERVER = os.getenv('SQL_SERVER', 'localhost')
SQL_DATABASE = os.getenv('SQL_DATABASE', 'ViterbitDB')

# Configuración de timeout y conexiones
TIMEOUT = ClientTimeout(total=120)
MAX_CONNECTIONS = 100

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

def create_ultima_actualizacion_table():
    """Crea la tabla Ultima_Actualizacion si no existe.
    
    Esta tabla almacena el registro de la última vez que se ejecutó cada script,
    permitiendo realizar actualizaciones incrementales.
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
        log_message(f"Error al crear/verificar la tabla Ultima_Actualizacion: {e}")
    finally:
        cursor.close()
        conn.close()

def create_tables():
    """Crea todas las tablas necesarias para el funcionamiento del script.
    
    Esta función:
    1. Crea la tabla Ultima_Actualizacion para el control de ejecuciones
    2. Crea la tabla Candidatures para almacenar las candidaturas
    3. Crea la tabla Stages_History para el historial de etapas
    """
    conn = get_db_connection()
    if not conn:
        return
    
    cursor = conn.cursor()
    try:
        # Crear tabla Ultima_Actualizacion si no existe
        create_ultima_actualizacion_table()
        
        # Tabla de Candidaturas
        cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Candidatures' AND xtype='U')
        BEGIN
            CREATE TABLE Candidatures (
                id VARCHAR(50) PRIMARY KEY,
                status VARCHAR(50),
                is_applied BIT,
                score DECIMAL(10,2),
                current_stage_id VARCHAR(50),
                current_stage_name NVARCHAR(255),
                current_stage_type_id VARCHAR(50),
                disqualified_at DATETIME,
                disqualified_by_id VARCHAR(50),
                disqualified_reason NVARCHAR(MAX),
                hired_at DATETIME,
                hired_by_id VARCHAR(50),
                hired_start_at DATETIME,
                hired_salary DECIMAL(18,2),
                hired_currency VARCHAR(10),
                hired_salary_periodicity VARCHAR(50),
                job_id VARCHAR(50),
                candidate_id VARCHAR(50),
                created_at DATETIME,
                created_by_id VARCHAR(50),
                updated_at DATETIME,
                custom_fields NVARCHAR(MAX)
            )
        END
        """)

        # Tabla de Historial de Etapas
        cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='CandidatureStagesHistory' AND xtype='U')
        BEGIN
            CREATE TABLE CandidatureStagesHistory (
                id INT IDENTITY(1,1) PRIMARY KEY,
                candidature_id VARCHAR(50),
                stage_name NVARCHAR(255),
                stage_type_id VARCHAR(50),
                start_at DATETIME,
                ends_at DATETIME,
                created_by_id VARCHAR(50),
                custom_fields NVARCHAR(MAX),
                FOREIGN KEY (candidature_id) REFERENCES Candidatures(id)
            )
        END
        """)
        
        conn.commit()
        log_message("✅ Tablas verificadas/creadas exitosamente")
    except Exception as e:
        log_message(f"❌ Error al crear/verificar tablas: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

def clear_existing_data():
    """Limpia los datos existentes en las tablas de candidaturas y etapas.
    
    Esta función se utiliza para realizar una carga limpia de datos,
    eliminando todos los registros existentes antes de insertar los nuevos.
    """
    conn = get_db_connection()
    if not conn:
        return
    
    cursor = conn.cursor()
    try:
        # Eliminar datos de las tablas
        cursor.execute("DELETE FROM Stages_History")
        cursor.execute("DELETE FROM Candidatures")
        conn.commit()
        log_message("🗑️ Datos existentes eliminados exitosamente")
    except Exception as e:
        log_message(f"Error al limpiar datos existentes: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

def registrar_ejecucion():
    """Registra la ejecución actual en la tabla Ultima_Actualizacion.
    
    Esta función actualiza o inserta el registro de la última ejecución exitosa,
    permitiendo realizar actualizaciones incrementales en futuras ejecuciones.
    """
    conn = get_db_connection()
    if not conn:
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
        """, (fecha_actual, 'candidatures.py'))
        
        # Si no se actualizó ninguna fila, insertar
        if cursor.rowcount == 0:
            cursor.execute("""
            INSERT INTO Ultima_Actualizacion (nombre_script, ultima_actualizacion)
            VALUES (?, ?)
            """, ('candidatures.py', fecha_actual))
        
        conn.commit()
        log_message(f"✅ Actualizada última ejecución: {fecha_actual.strftime('%Y-%m-%d %H:%M:%S')}")
    except Exception as e:
        log_message(f"❌ Error al actualizar última ejecución: {e}")
        conn.rollback()
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
        cursor.execute("""
            SELECT ultima_actualizacion 
            FROM Ultima_Actualizacion 
            WHERE nombre_script = ?
        """, ('candidatures.py',))
        
        result = cursor.fetchone()
        if result and result[0]:
            fecha_dt = result[0]
            # Restar un día a la fecha para asegurar que no se pierdan registros
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

async def get_page_by_date_async(session: aiohttp.ClientSession, page_number: int, fecha_desde: str, date_type: str) -> tuple:
    """Obtiene una página de candidaturas de forma asíncrona para un tipo específico de fecha.
    
    Args:
        session (aiohttp.ClientSession): Sesión HTTP para realizar las peticiones.
        page_number (int): Número de página a obtener.
        fecha_desde (str): Fecha desde la cual obtener las candidaturas.
        date_type (str): Tipo de fecha a filtrar ('created_after' o 'updated_after').
    
    Returns:
        tuple: (lista de candidaturas, indicador de si hay más páginas)
    """
    url = f"{CANDIDATURES_API_URL}?page_size={PAGE_SIZE}&page={page_number}"
    failed_pages = set()  # Conjunto para almacenar páginas que fallaron
    
    # Solo añadir el parámetro de fecha si hay una fecha válida
    if fecha_desde and fecha_desde.strip():
        fecha_encoded = fecha_desde.replace(" ", "%20").replace(":", "%3A")
        url += f"&{date_type}={fecha_encoded}"
    
    for attempt in range(MAX_RETRIES):
        try:
            async with session.get(url, headers=HEADERS) as response:
                if response.status == 200:
                    data = await response.json()
                    candidatures = data.get("data", [])
                    has_more = data.get("meta", {}).get("has_more", False)
                    total_count = data.get("meta", {}).get("total", 0)
                    
                    if page_number == 1:
                        log_message(f"📊 Total de registros para {date_type}: {total_count}")
                    
                    log_message(f"📡 Página {page_number} ({date_type}): Obtenidas {len(candidatures)} candidaturas")
                    return candidatures, has_more
                elif response.status == 429:  # Rate limit
                    retry_after = int(response.headers.get('Retry-After', RETRY_DELAY))
                    log_message(f"Rate limit alcanzado, esperando {retry_after} segundos")
                    await asyncio.sleep(retry_after)
                    continue
                elif response.status == 422:  # Unprocessable Entity
                    error_text = await response.text()
                    log_message(f"⚠️ Error de validación en la página {page_number} ({date_type}): {error_text}")
                    failed_pages.add(page_number)
                    await asyncio.sleep(RETRY_DELAY * (attempt + 1))
                    continue
                else:
                    error_text = await response.text()
                    log_message(f"Error {response.status} en la página {page_number} ({date_type})")
                    log_message(f"URL: {url}")
                    log_message(f"Respuesta: {error_text}")
                    
                    if attempt < MAX_RETRIES - 1:
                        await asyncio.sleep(RETRY_DELAY * (attempt + 1))
                        continue
                    return [], False
        except Exception as e:
            log_message(f"Error en la página {page_number} ({date_type}): {str(e)}")
            if attempt < MAX_RETRIES - 1:
                await asyncio.sleep(RETRY_DELAY * (attempt + 1))
                continue
            return [], False
    
    return [], False

async def get_all_candidatures_by_date_type(session: aiohttp.ClientSession, fecha_desde: str) -> List[Dict[str, Any]]:
    """Obtiene todas las candidaturas para los tipos de fecha especificados"""
    all_candidatures = []
    date_types = ["created_after", "updated_after"]
    
    for date_type in date_types:
        log_message(f"🔄 Obteniendo candidaturas para {date_type}...")
        page = 1
        
        while True:
            candidatures, has_more = await get_page_by_date_async(session, page, fecha_desde, date_type)
            if not candidatures:
                break
            
            all_candidatures.extend(candidatures)
            
            # Verificar si hemos alcanzado el límite
            if LIMIT_RECORDS > 0 and len(all_candidatures) >= LIMIT_RECORDS:
                all_candidatures = all_candidatures[:LIMIT_RECORDS]
                log_message(f"📊 Límite de registros alcanzado ({LIMIT_RECORDS})")
                break
            
            if not has_more:
                break
                
            page += 1
            await asyncio.sleep(0.3)  # Pausa entre páginas
    
    # Eliminar duplicados basados en el ID de la candidatura
    unique_candidatures = {c['id']: c for c in all_candidatures}.values()
    return list(unique_candidatures)

async def get_candidature_details_async(session: aiohttp.ClientSession, candidature_id: str) -> Dict[str, Any]:
    """Obtiene los detalles completos de una candidatura de forma asíncrona.
    
    Args:
        session (aiohttp.ClientSession): Sesión HTTP para realizar las peticiones.
        candidature_id (str): ID de la candidatura a obtener.
    
    Returns:
        Dict[str, Any]: Datos de la candidatura o None si hay error.
    """
    url = f"{CANDIDATURES_API_URL}/{candidature_id}"
    retry_count = 0
    
    while retry_count < MAX_RETRIES:
        try:
            async with session.get(url, headers=HEADERS) as response:
                if response.status == 200:
                    data = await response.json()
                    candidature_data = data.get('data', {})
                    if not candidature_data:
                        return None
                    return candidature_data
                elif response.status == 429:  # Rate limit
                    retry_after = int(response.headers.get('Retry-After', RETRY_DELAY))
                    await asyncio.sleep(retry_after)
                    retry_count += 1
                    continue
                else:
                    if retry_count < MAX_RETRIES - 1:
                        await asyncio.sleep(RETRY_DELAY * (retry_count + 1))
                        retry_count += 1
                        continue
                    return None
        except Exception as e:
            if retry_count < MAX_RETRIES - 1:
                await asyncio.sleep(RETRY_DELAY * (retry_count + 1))
                retry_count += 1
                continue
            return None
    return None

async def process_candidature_batch(session: aiohttp.ClientSession, candidatures: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Procesa un lote de candidaturas de forma asíncrona"""
    # Dividir el lote en sub-lotes más pequeños para procesamiento concurrente
    sub_batch_size = 10  # Procesar 10 candidaturas a la vez
    all_results = []
    
    for i in range(0, len(candidatures), sub_batch_size):
        sub_batch = candidatures[i:i + sub_batch_size]
        tasks = [get_candidature_details_async(session, cand['id']) for cand in sub_batch]
        results = await asyncio.gather(*tasks)
        all_results.extend([r for r in results if r is not None])
        await asyncio.sleep(0.1)  # Pequeña pausa entre sub-lotes
    
    return all_results

def format_date(date_str: Optional[str]) -> Optional[datetime]:
    """Formatea una cadena de fecha a objeto datetime.
    
    Esta función maneja múltiples formatos de fecha y realiza validaciones
    para asegurar que las fechas estén dentro del rango válido de SQL Server.
    
    Args:
        date_str (Optional[str]): Cadena de fecha a formatear.
    
    Returns:
        Optional[datetime]: Objeto datetime formateado o None si hay error.
    """
    if not date_str:
        return None
    try:
        # Limpiar la cadena de fecha
        date_str = str(date_str).strip().replace('Z', '+00:00')
        
        # Intentar diferentes formatos de fecha
        formats = [
            '%Y-%m-%dT%H:%M:%S.%fZ',  # ISO con milisegundos
            '%Y-%m-%dT%H:%M:%SZ',     # ISO sin milisegundos
            '%Y-%m-%d %H:%M:%S',      # Formato SQL Server
            '%Y-%m-%d'                # Solo fecha
        ]
        
        for fmt in formats:
            try:
                dt = datetime.strptime(date_str, fmt)
                # Validar que la fecha esté dentro del rango válido de SQL Server
                if dt.year < 1753:
                    log_message(f"⚠️ Fecha anterior a 1753 encontrada: {date_str}, usando fecha mínima permitida")
                    return datetime(1753, 1, 1)
                elif dt.year > 9999:
                    log_message(f"⚠️ Fecha posterior a 9999 encontrada: {date_str}, usando fecha máxima permitida")
                    return datetime(9999, 12, 31, 23, 59, 59)
                return dt
            except ValueError:
                continue
        
        # Si ningún formato funciona, intentar parsear con dateutil
        from dateutil import parser
        dt = parser.parse(date_str)
        if dt.year < 1753:
            log_message(f"⚠️ Fecha anterior a 1753 encontrada: {date_str}, usando fecha mínima permitida")
            return datetime(1753, 1, 1)
        elif dt.year > 9999:
            log_message(f"⚠️ Fecha posterior a 9999 encontrada: {date_str}, usando fecha máxima permitida")
            return datetime(9999, 12, 31, 23, 59, 59)
        return dt
    except Exception as e:
        log_message(f"⚠️ Error al formatear fecha '{date_str}': {str(e)}")
        return None

def save_to_database(candidatures: List[Dict[str, Any]], stages_history: List[Dict[str, Any]]):
    """Guarda las candidaturas y su historial de etapas en la base de datos.
    
    Esta función:
    1. Guarda las candidaturas en la tabla Candidatures
    2. Guarda el historial de etapas en la tabla Stages_History
    3. Maneja errores y reintentos para cada registro
    4. Proporciona un resumen detallado del proceso
    
    Args:
        candidatures (List[Dict[str, Any]]): Lista de candidaturas a guardar.
        stages_history (List[Dict[str, Any]]): Lista de registros de historial de etapas.
    """
    conn = get_db_connection()
    if not conn:
        return
    
    cursor = conn.cursor()
    try:
        # Insertar o actualizar candidaturas
        for candidature in candidatures:
            candidature_id = candidature.get('id')
            if not candidature_id:
                log_message(f"⚠️ Candidatura sin ID, saltando...")
                continue

            # Formatear fechas
            created_at = format_date(candidature.get('created_at'))
            updated_at = format_date(candidature.get('updated_at'))
            disqualified_at = format_date(candidature.get('disqualified_info', {}).get('disqualified_at'))
            hired_at = format_date(candidature.get('hired_info', {}).get('hired_at'))
            hired_start_at = format_date(candidature.get('hired_info', {}).get('start_at'))
            
            # Verificar si la candidatura ya existe
            cursor.execute("SELECT id FROM Candidatures WHERE id = ?", (candidature_id,))
            exists = cursor.fetchone() is not None
            
            if exists:
                log_message(f"🔄 Actualizando candidatura ID: {candidature_id}")
            
            cursor.execute("""
            MERGE INTO Candidatures WITH (HOLDLOCK) AS target
            USING (VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)) AS source 
                (id, status, is_applied, score, current_stage_id, current_stage_name, 
                current_stage_type_id, disqualified_at, disqualified_by_id, disqualified_reason,
                hired_at, hired_by_id, hired_start_at, hired_salary, hired_currency,
                hired_salary_periodicity, job_id, candidate_id, created_at, created_by_id,
                updated_at, custom_fields)
            ON target.id = source.id
            WHEN MATCHED THEN
                UPDATE SET 
                    status = source.status,
                    is_applied = source.is_applied,
                    score = source.score,
                    current_stage_id = source.current_stage_id,
                    current_stage_name = source.current_stage_name,
                    current_stage_type_id = source.current_stage_type_id,
                    disqualified_at = source.disqualified_at,
                    disqualified_by_id = source.disqualified_by_id,
                    disqualified_reason = source.disqualified_reason,
                    hired_at = source.hired_at,
                    hired_by_id = source.hired_by_id,
                    hired_start_at = source.hired_start_at,
                    hired_salary = source.hired_salary,
                    hired_currency = source.hired_currency,
                    hired_salary_periodicity = source.hired_salary_periodicity,
                    job_id = source.job_id,
                    candidate_id = source.candidate_id,
                    created_at = source.created_at,
                    created_by_id = source.created_by_id,
                    updated_at = source.updated_at,
                    custom_fields = source.custom_fields
            WHEN NOT MATCHED THEN
                INSERT (id, status, is_applied, score, current_stage_id, current_stage_name,
                        current_stage_type_id, disqualified_at, disqualified_by_id, disqualified_reason,
                        hired_at, hired_by_id, hired_start_at, hired_salary, hired_currency,
                        hired_salary_periodicity, job_id, candidate_id, created_at, created_by_id,
                        updated_at, custom_fields)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """, (
                candidature_id,
                candidature.get('status'),
                candidature.get('is_applied', False),
                candidature.get('score', 0),
                candidature.get('current_stage', {}).get('id'),
                candidature.get('current_stage', {}).get('name'),
                candidature.get('current_stage', {}).get('stage_type_id'),
                disqualified_at,
                candidature.get('disqualified_info', {}).get('disqualified_by_id'),
                candidature.get('disqualified_info', {}).get('reason'),
                hired_at,
                candidature.get('hired_info', {}).get('hired_by_id'),
                hired_start_at,
                candidature.get('hired_info', {}).get('salary'),
                candidature.get('hired_info', {}).get('currency'),
                candidature.get('hired_info', {}).get('salary_periodicity'),
                candidature.get('job_id'),
                candidature.get('candidate_id'),
                created_at,
                candidature.get('created_by_id'),
                updated_at,
                json.dumps(candidature.get('custom_fields', []), ensure_ascii=False),
                # Valores para el INSERT
                candidature_id,
                candidature.get('status'),
                candidature.get('is_applied', False),
                candidature.get('score', 0),
                candidature.get('current_stage', {}).get('id'),
                candidature.get('current_stage', {}).get('name'),
                candidature.get('current_stage', {}).get('stage_type_id'),
                disqualified_at,
                candidature.get('disqualified_info', {}).get('disqualified_by_id'),
                candidature.get('disqualified_info', {}).get('reason'),
                hired_at,
                candidature.get('hired_info', {}).get('hired_by_id'),
                hired_start_at,
                candidature.get('hired_info', {}).get('salary'),
                candidature.get('hired_info', {}).get('currency'),
                candidature.get('hired_info', {}).get('salary_periodicity'),
                candidature.get('job_id'),
                candidature.get('candidate_id'),
                created_at,
                candidature.get('created_by_id'),
                updated_at,
                json.dumps(candidature.get('custom_fields', []), ensure_ascii=False)
            ))

        # Insertar historial de etapas
        for stage in stages_history:
            candidature_id = stage.get('candidature_id')
            if not candidature_id:
                log_message(f"⚠️ Etapa sin candidature_id, saltando...")
                continue

            # Formatear fechas
            start_at = format_date(stage.get('start_at'))
            ends_at = format_date(stage.get('ends_at'))
            
            # Verificar si la etapa ya existe para esta candidatura
            cursor.execute("""
                SELECT id FROM CandidatureStagesHistory 
                WHERE candidature_id = ? AND stage_name = ? AND start_at = ?
            """, (candidature_id, stage.get('stage_name'), start_at))
            
            if cursor.fetchone() is None:
                cursor.execute("""
                INSERT INTO CandidatureStagesHistory 
                    (candidature_id, stage_name, stage_type_id, start_at, ends_at, 
                    created_by_id, custom_fields)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    candidature_id,
                    stage.get('stage_name'),
                    stage.get('stage_type_id'),
                    start_at,
                    ends_at,
                    stage.get('created_by_id'),
                    json.dumps(stage.get('custom_fields', []), ensure_ascii=False)
                ))

        conn.commit()
        log_message(f"✅ Guardados {len(candidatures)} candidaturas y {len(stages_history)} registros de historial")
    except Exception as e:
        log_message(f"❌ Error al guardar en la base de datos: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

async def get_page_simple_async(session: aiohttp.ClientSession, page_number: int) -> tuple:
    """Obtiene una página de candidaturas de forma asíncrona sin filtros de fecha"""
    url = f"{CANDIDATURES_API_URL}?page_size={PAGE_SIZE}&page={page_number}"
    
    for attempt in range(MAX_RETRIES):
        try:
            async with session.get(url, headers=HEADERS) as response:
                if response.status == 200:
                    data = await response.json()
                    candidatures = data.get("data", [])
                    has_more = data.get("meta", {}).get("has_more", False)
                    total_count = data.get("meta", {}).get("total", 0)
                    
                    if page_number == 1:
                        log_message(f"📡 Total de registros en la API: {total_count}")
                    
                    # Calcular el total acumulado de candidaturas recuperadas
                    candidaturas_acumuladas = page_number * PAGE_SIZE
                    if candidaturas_acumuladas > total_count:
                        candidaturas_acumuladas = total_count
                    
                    # Calcular el porcentaje de progreso
                    porcentaje = (candidaturas_acumuladas / total_count) * 100 if total_count > 0 else 0
                    
                    log_message(f"📡 Página {page_number}: Obtenidos {len(candidatures)} de {total_count} candidaturas")
                    return candidatures, has_more
                elif response.status == 429:  # Rate limit
                    retry_after = int(response.headers.get('Retry-After', RETRY_DELAY))
                    log_message(f"Rate limit alcanzado, esperando {retry_after} segundos")
                    await asyncio.sleep(retry_after)
                    continue
                elif response.status == 401:
                    log_message("Error de autenticación (401). Verificar API key")
                    return [], False
                elif response.status == 403:
                    log_message("Error de autorización (403). Sin permisos para acceder")
                    return [], False
                else:
                    error_text = await response.text()
                    log_message(f"Error {response.status} en la página {page_number}")
                    log_message(f"URL: {url}")
                    log_message(f"Headers de respuesta: {dict(response.headers)}")
                    log_message(f"Respuesta: {error_text}")
                    
                    if attempt < MAX_RETRIES - 1:
                        await asyncio.sleep(RETRY_DELAY * (attempt + 1))
                        continue
                    return [], False
        except Exception as e:
            log_message(f"Error en la página {page_number}: {str(e)}")
            if attempt < MAX_RETRIES - 1:
                await asyncio.sleep(RETRY_DELAY * (attempt + 1))
                continue
            return [], False
    
    return [], False

async def get_all_candidatures_simple(session: aiohttp.ClientSession) -> List[Dict[str, Any]]:
    """Obtiene todas las candidaturas sin filtros de fecha de forma concurrente"""
    # Primero obtenemos la primera página para saber el total
    first_page, has_more = await get_page_simple_async(session, 1)
    if not first_page:
        return []
    
    # Obtener el total_count de la respuesta de la API
    url = f"{CANDIDATURES_API_URL}?page_size={PAGE_SIZE}&page=1"
    async with session.get(url, headers=HEADERS) as response:
        if response.status == 200:
            data = await response.json()
            total_count = data.get("meta", {}).get("total", 0)
        else:
            return first_page
    
    # Aplicar límite si está configurado
    if LIMIT_RECORDS > 0:
        total_count = min(total_count, LIMIT_RECORDS)
        log_message(f"📊 Límite de registros configurado: {LIMIT_RECORDS}")
    
    if has_more and total_count > 0:
        # Calcular el número total de páginas basado en el total_count
        total_pages = (total_count + PAGE_SIZE - 1) // PAGE_SIZE
        
        log_message(f"📊 Total de páginas a procesar: {total_pages}")
        
        # Crear tareas para obtener las páginas restantes de forma concurrente
        tasks = []
        for page in range(2, total_pages + 1):
            tasks.append(get_page_simple_async(session, page))
        
        # Ejecutar todas las tareas de forma concurrente
        results = await asyncio.gather(*tasks)
        
        # Procesar resultados
        all_candidatures = first_page
        for candidatures, _ in results:
            if candidatures:
                all_candidatures.extend(candidatures)
        
        # Aplicar límite al resultado final si está configurado
        if LIMIT_RECORDS > 0:
            all_candidatures = all_candidatures[:LIMIT_RECORDS]
        
        return all_candidatures
    else:
        return first_page

async def get_candidatures_from_api(last_update: Optional[str] = None) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Obtiene candidaturas de la API de forma asíncrona.
    
    Esta función:
    1. Obtiene candidaturas creadas y actualizadas desde la última ejecución
    2. Maneja la paginación y los rate limits de la API
    3. Procesa los datos en paralelo para mejorar el rendimiento
    4. Retorna las candidaturas y su historial de etapas
    
    Args:
        last_update (Optional[str]): Fecha de la última actualización en formato ISO.
    
    Returns:
        Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]: Tupla con (candidaturas, historial de etapas)
    """
    all_candidatures = []
    all_stages_history = []
    failed_candidatures = set()
    
    async with aiohttp.ClientSession() as session:
        # Obtener candidaturas creadas después de la última actualización
        created_candidatures = await get_candidatures_by_date_type(session, last_update, 'created_after')
        all_candidatures.extend(created_candidatures)
        
        # Obtener candidaturas actualizadas después de la última actualización
        updated_candidatures = await get_candidatures_by_date_type(session, last_update, 'updated_after')
        all_candidatures.extend(updated_candidatures)
        
        # Eliminar duplicados basados en el ID
        unique_candidatures = {c['id']: c for c in all_candidatures}.values()
        all_candidatures = list(unique_candidatures)
        
        if not all_candidatures:
            log_message("ℹ️ No se encontraron candidaturas nuevas o actualizadas")
            return [], []
        
        log_message(f"📊 Total de candidaturas únicas a procesar: {len(all_candidatures)}")
        
        # Procesar candidaturas en paralelo
        tasks = []
        for candidature in all_candidatures:
            task = asyncio.create_task(get_candidature_details_async(session, candidature['id']))
            tasks.append(task)
        
        # Esperar a que todas las tareas terminen
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Procesar resultados
        for candidature, result in zip(all_candidatures, results):
            if isinstance(result, Exception):
                log_message(f"❌ Error al obtener detalles de candidatura {candidature['id']}: {str(result)}")
                failed_candidatures.add(candidature['id'])
                continue
            
            if not result:
                log_message(f"⚠️ No se obtuvieron detalles para candidatura {candidature['id']}")
                failed_candidatures.add(candidature['id'])
                continue
            
            # Extraer historial de etapas
            stages = result.get('stages_history', [])
            for stage in stages:
                stage['candidature_id'] = candidature['id']
                all_stages_history.append(stage)
        
        if failed_candidatures:
            log_message(f"⚠️ {len(failed_candidatures)} candidaturas fallaron al obtener detalles")
            log_message(f"IDs fallidos: {', '.join(failed_candidatures)}")
        
        return all_candidatures, all_stages_history

async def get_candidatures_by_date_type(session: aiohttp.ClientSession, last_update: Optional[str], date_type: str) -> List[Dict[str, Any]]:
    """Obtiene candidaturas filtradas por tipo de fecha de forma asíncrona.
    
    Args:
        session (aiohttp.ClientSession): Sesión HTTP para realizar las peticiones.
        last_update (Optional[str]): Fecha de la última actualización.
        date_type (str): Tipo de fecha a filtrar ('created_after' o 'updated_after').
    
    Returns:
        List[Dict[str, Any]]: Lista de candidaturas obtenidas.
    """
    candidatures = []
    page = 1
    has_more = True
    
    while has_more:
        page_candidatures, has_more = await get_page_by_date_async(session, page, last_update, date_type)
        if page_candidatures:
            candidatures.extend(page_candidatures)
        page += 1
    
    return candidatures

async def main():
    """Función principal que orquesta el proceso de obtención y guardado de candidaturas.
    
    Esta función:
    1. Crea las tablas necesarias en la base de datos
    2. Obtiene la fecha de la última actualización
    3. Obtiene las candidaturas de la API
    4. Guarda las candidaturas y su historial en la base de datos
    5. Registra la ejecución exitosa
    """
    try:
        # Crear tablas si no existen
        create_tables()
        
        # Obtener última actualización
        last_update = get_last_update()
        if last_update:
            log_message(f"📅 Obteniendo candidaturas desde: {last_update}")
        
        # Obtener candidaturas de la API
        candidatures, stages_history = await get_candidatures_from_api(last_update)
        
        if not candidatures:
            log_message("ℹ️ No hay candidaturas para procesar")
            return
        
        # Guardar en base de datos
        save_to_database(candidatures, stages_history)
        
    except Exception as e:
        log_message(f"❌ Error en la ejecución principal: {str(e)}")
        raise

if __name__ == "__main__":
    # Configurar el capturador de logs
    log_capture = LogCapture()
    
    # Ejecutar el proceso principal
    asyncio.run(main())
    
    # Obtener y mostrar el contenido del log
    log_content = log_capture._get_last_log_content()
    if log_content:
        print("\nResumen de la ejecución:")
        print(log_content)
