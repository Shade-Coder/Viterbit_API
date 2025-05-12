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
from typing import List, Dict, Any, Optional
import sys
from io import StringIO
import shutil

# Variables globales para agrupar logs
error_pages = set()
retry_pages = set()
wait_messages = set()
last_log_time = None
problematic_pages = set()  # Conjunto para almacenar páginas problemáticas durante la ejecución actual

class LogCapture:
    def __init__(self):
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        # Crear carpeta logs si no existe
        if not os.path.exists('logs'):
            os.makedirs('logs')
        self.log_file = os.path.join('logs', f'Candidates_execution_log_{self.timestamp}.txt')
        self.original_stdout = sys.stdout
        self.captured_output = StringIO()
        # Crear un objeto que escriba tanto en el buffer como en la consola original
        self.tee = TeeOutput(self.original_stdout, self.captured_output)
        sys.stdout = self.tee

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
            # Eliminar logs anteriores
            for old_log in os.listdir('logs'):
                if old_log.startswith('Candidates_execution_log_') and old_log.endswith('.txt'):
                    try:
                        os.remove(os.path.join('logs', old_log))
                    except Exception as e:
                        print(f"Error al eliminar log anterior: {e}")
            
            # Guardar el nuevo log
            with open(self.log_file, 'w', encoding='utf-8') as f:
                f.write(self.captured_output.getvalue())
        
        # Limpiar el buffer
        self.captured_output.close()

class TeeOutput:
    """Clase que escribe en múltiples streams simultáneamente"""
    def __init__(self, *streams):
        self.streams = streams

    def write(self, data):
        for stream in self.streams:
            stream.write(data)

    def flush(self):
        for stream in self.streams:
            stream.flush()

def log_message(message: str, force_immediate: bool = False) -> None:
    """Imprime un mensaje con timestamp, agrupando mensajes similares"""
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
        # Solo mostrar páginas que realmente necesitan reintento
        page = message.split("página")[-1].strip()
        if page in error_pages:  # Solo agregar si la página tuvo error
            retry_pages.add(page)
            if force_immediate or (last_log_time and (current_time - last_log_time).seconds >= 2):
                log_entry = f"[{timestamp}] 🔄 Intento {message.split('Intento')[1].split('para')[0].strip()} para las páginas: {', '.join(sorted(retry_pages))}"
                print(log_entry)
                retry_pages.clear()
                last_log_time = current_time
    elif "Esperando" in message:
        # Extraer el tiempo de espera del mensaje
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
        # Para otros mensajes, imprimir inmediatamente
        log_entry = f"[{timestamp}] {message}"
        print(log_entry)
        last_log_time = current_time

# Cargar variables de entorno
load_dotenv()

# Configuración de la API
VITERBIT_BASE_URL = "https://api.viterbit.com/v1"
CANDIDATES_API_URL = f"{VITERBIT_BASE_URL}/candidates"
API_KEY = os.getenv('VITERBIT_API_KEY')
HEADERS = {"x-api-key": API_KEY}

# Configuración de API
MAX_RETRIES = 5
RETRY_DELAY = 2
PAGE_SIZE = 100
CONCURRENT_REQUESTS = 100
BATCH_SIZE = 100
LIMIT_RECORDS =1000 # Número máximo de candidatos a recuperar (0 para recuperar todos)

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
    Esta tabla almacena el registro de la última vez que se ejecutó cada script.
    """
    conn = get_db_connection()
    if not conn:
        return
    
    cursor = conn.cursor()
    try:
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
        cursor.execute("""
            SELECT ultima_actualizacion 
            FROM Ultima_Actualizacion 
            WHERE nombre_script = ?
        """, ('candidates.py',))
        
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

def update_last_execution():
    """Actualiza o inserta el registro de la última ejecución exitosa en la base de datos.
    Esta función se llama solo cuando el proceso se completa sin errores.
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
        """, (fecha_actual, 'candidates.py'))
        
        # Si no se actualizó ninguna fila, insertar
        if cursor.rowcount == 0:
            cursor.execute("""
            INSERT INTO Ultima_Actualizacion (nombre_script, ultima_actualizacion)
            VALUES (?, ?)
            """, ('candidates.py', fecha_actual))
        
        conn.commit()
        log_message(f"✅ Actualizada última ejecución: {fecha_actual.strftime('%Y-%m-%d %H:%M:%S')}")
    except Exception as e:
        log_message(f"❌ Error al actualizar última ejecución: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

def create_candidates_table():
    """Crea la tabla Candidates si no existe.
    Esta tabla almacena toda la información de los candidatos obtenida de la API.
    """
    conn = get_db_connection()
    if not conn:
        return
    
    cursor = conn.cursor()
    try:
        # Verificar si la tabla existe
        cursor.execute("""
            SELECT COUNT(*) 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_NAME = 'Candidates'
        """)
        table_exists = cursor.fetchone()[0] > 0

        if not table_exists:
            # Crear la tabla con la estructura exacta del JSON
            cursor.execute("""
            CREATE TABLE Candidates (
                id VARCHAR(50) PRIMARY KEY,
                reference VARCHAR(100),
                full_name NVARCHAR(MAX),
                phone VARCHAR(50),
                email NVARCHAR(255),
                gender VARCHAR(50),
                birthday DATETIME,
                picture_url NVARCHAR(MAX),
                source VARCHAR(100),
                source_params NVARCHAR(MAX),
                is_applied BIT,
                talent_community_signed_at DATETIME,
                created_at DATETIME,
                updated_at DATETIME,
                created_by_id VARCHAR(50),
                tags NVARCHAR(MAX),
                social_profile_linkedin NVARCHAR(MAX),
                social_profile_facebook NVARCHAR(MAX),
                social_profile_twitter NVARCHAR(MAX),
                social_profile_github NVARCHAR(MAX),
                social_profile_tiktok NVARCHAR(MAX),
                social_profile_instagram NVARCHAR(MAX),
                social_profile_other NVARCHAR(MAX)
            )
            """)
            conn.commit()
            log_message("💾 Tabla Candidates creada exitosamente")
    except Exception as e:
        log_message(f"Error al crear la tabla: {e}")
    finally:
        cursor.close()
        conn.close()

def create_tables():
    """Crea todas las tablas necesarias para el funcionamiento del script.
    Incluye la tabla Candidates y Ultima_Actualizacion.
    """
    create_candidates_table()
    create_ultima_actualizacion_table()

def flatten_candidate(candidate: Dict[str, Any]) -> Dict[str, Any]:
    """Aplana y formatea la estructura del candidato para su almacenamiento en la base de datos.
    
    Args:
        candidate (Dict[str, Any]): Diccionario con los datos del candidato de la API.
    
    Returns:
        Dict[str, Any]: Diccionario con los datos formateados y aplanados, o None si hay error.
    """
    try:
        # Función para formatear fechas
        def format_date(date_str):
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
                            return '1753-01-01 00:00:00'
                        elif dt.year > 9999:
                            log_message(f"⚠️ Fecha posterior a 9999 encontrada: {date_str}, usando fecha máxima permitida")
                            return '9999-12-31 23:59:59'
                        return dt.strftime('%Y-%m-%d %H:%M:%S')
                    except ValueError:
                        continue
                
                # Si ningún formato funciona, intentar parsear con dateutil
                from dateutil import parser
                dt = parser.parse(date_str)
                if dt.year < 1753:
                    log_message(f"⚠️ Fecha anterior a 1753 encontrada: {date_str}, usando fecha mínima permitida")
                    return '1753-01-01 00:00:00'
                elif dt.year > 9999:
                    log_message(f"⚠️ Fecha posterior a 9999 encontrada: {date_str}, usando fecha máxima permitida")
                    return '9999-12-31 23:59:59'
                return dt.strftime('%Y-%m-%d %H:%M:%S')
            except Exception as e:
                log_message(f"⚠️ Error al formatear fecha '{date_str}': {str(e)}")
                return None

        # Procesar tags
        tags = candidate.get('tags', [])
        if isinstance(tags, list):
            tags_str = ', '.join(str(tag) for tag in tags)
        else:
            tags_str = ''

        # Procesar social profiles
        social_profiles = candidate.get('social_profiles', [])
        social_profile_dict = {
            'linkedin': '',
            'facebook': '',
            'twitter': '',
            'github': '',
            'tiktok': '',
            'instagram': '',
            'other': ''
        }

        if isinstance(social_profiles, list):
            for profile in social_profiles:
                if isinstance(profile, dict):
                    key = profile.get('key', '').lower()
                    value = profile.get('value', '')
                    
                    if key in social_profile_dict:
                        social_profile_dict[key] = value
                    else:
                        # Si es un perfil social no listado, lo guardamos en 'other'
                        if social_profile_dict['other']:
                            social_profile_dict['other'] += f", {key}: {value}"
                        else:
                            social_profile_dict['other'] = f"{key}: {value}"

        # Crear diccionario base con los campos exactos del JSON
        flattened = {
            'id': str(candidate.get('id', '')),
            'reference': str(candidate.get('reference', '')),
            'full_name': str(candidate.get('full_name', '')),
            'phone': str(candidate.get('phone', '')),
            'email': str(candidate.get('email', '')),
            'gender': str(candidate.get('gender', '')),
            'birthday': format_date(candidate.get('birthday')),
            'picture_url': str(candidate.get('picture_url', '')),
            'source': str(candidate.get('source', '')),
            'source_params': json.dumps(candidate.get('source_params', {}), ensure_ascii=False),
            'is_applied': 1 if candidate.get('is_applied') else 0,
            'talent_community_signed_at': format_date(candidate.get('talent_community_signed_at')),
            'created_at': format_date(candidate.get('created_at')),
            'updated_at': format_date(candidate.get('updated_at')),
            'created_by_id': str(candidate.get('created_by_id', '')),
            'tags': tags_str,
            'social_profile_linkedin': social_profile_dict['linkedin'],
            'social_profile_facebook': social_profile_dict['facebook'],
            'social_profile_twitter': social_profile_dict['twitter'],
            'social_profile_github': social_profile_dict['github'],
            'social_profile_tiktok': social_profile_dict['tiktok'],
            'social_profile_instagram': social_profile_dict['instagram'],
            'social_profile_other': social_profile_dict['other']
        }
        
        return flattened
    except Exception as e:
        log_message(f"Error al aplanar el candidato: {e}")
        return None

def save_to_database(candidates: List[Dict[str, Any]]) -> None:
    """Guarda los candidatos en la base de datos de forma optimizada.
    
    Args:
        candidates (List[Dict[str, Any]]): Lista de candidatos a guardar.
    
    Raises:
        Exception: Si no se puede conectar a la base de datos o hay un error al guardar.
    """
    conn = get_db_connection()
    if not conn:
        log_message("No se pudo conectar a la base de datos.")
        raise Exception("No se pudo conectar a la base de datos")
    
    cursor = conn.cursor()
    try:
        # Verificar si existe la tabla Candidates
        cursor.execute("""
            SELECT COUNT(*) 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_NAME = 'Candidates'
        """)
        table_exists = cursor.fetchone()[0] > 0

        # Obtener última fecha de actualización
        last_update = get_last_update()

        if not table_exists:
            log_message("💾 Creando nueva tabla Candidates...")
            # Crear la tabla con la estructura exacta del JSON
            cursor.execute("""
            CREATE TABLE Candidates (
                id VARCHAR(50) PRIMARY KEY,
                reference VARCHAR(100),
                full_name NVARCHAR(MAX),
                phone VARCHAR(50),
                email NVARCHAR(255),
                gender VARCHAR(50),
                birthday DATETIME,
                picture_url NVARCHAR(MAX),
                source VARCHAR(100),
                source_params NVARCHAR(MAX),
                is_applied BIT,
                talent_community_signed_at DATETIME,
                created_at DATETIME,
                updated_at DATETIME,
                created_by_id VARCHAR(50),
                tags NVARCHAR(MAX),
                social_profile_linkedin NVARCHAR(MAX),
                social_profile_facebook NVARCHAR(MAX),
                social_profile_twitter NVARCHAR(MAX),
                social_profile_github NVARCHAR(MAX),
                social_profile_tiktok NVARCHAR(MAX),
                social_profile_instagram NVARCHAR(MAX),
                social_profile_other NVARCHAR(MAX)
            )
            """)
            log_message("💾 Tabla Candidates creada exitosamente")
        
        # Preparar datos en lotes
        failed_candidates = []
        flat_candidates = []
        retry_candidates = []
        
        # Primera pasada: intentar procesar todos los candidatos
        for candidate in candidates:
            try:
                flat_candidate = flatten_candidate(candidate)
                if flat_candidate:
                    flat_candidates.append(flat_candidate)
                else:
                    retry_candidates.append(candidate)
            except Exception as e:
                retry_candidates.append(candidate)
        
        # Segunda pasada: reintentar los candidatos que fallaron
        if retry_candidates:
            log_message(f"🔄 Reintentando procesar {len(retry_candidates)} candidatos que fallaron en la primera pasada...")
            for candidate in retry_candidates:
                try:
                    flat_candidate = flatten_candidate(candidate)
                    if flat_candidate:
                        flat_candidates.append(flat_candidate)
                    else:
                        failed_candidates.append({
                            'id': candidate.get('id', 'N/A'),
                            'data': candidate,
                            'reason': 'Error al aplanar el candidato después de reintento'
                        })
                except Exception as e:
                    failed_candidates.append({
                        'id': candidate.get('id', 'N/A'),
                        'data': candidate,
                        'reason': f'Error al procesar después de reintento: {str(e)}'
                    })

        if not flat_candidates:
            log_message("No hay candidatos para guardar")
            return

        def process_with_reduced_batch_size(candidates, batch_size):
            """Procesa candidatos con un tamaño de lote reducido.
            
            Args:
                candidates (List[Dict]): Lista de candidatos a procesar.
                batch_size (int): Tamaño del lote a procesar.
            
            Returns:
                tuple: (lista de candidatos fallidos, total de candidatos procesados)
            """
            if not candidates:
                return [], 0
            
            total_processed = 0
            remaining_candidates = candidates
            failed_candidates = []
            
            while remaining_candidates:
                batch = remaining_candidates[:batch_size]
                remaining_candidates = remaining_candidates[batch_size:]
                
                log_message(f"\n🔄 Procesando lote de {batch_size} candidatos...")
                
                # Obtener IDs de los candidatos en el lote
                batch_ids = [c['id'] for c in batch]
                
                # Verificar IDs existentes en la base de datos
                cursor.execute("""
                    SELECT id FROM Candidates 
                    WHERE id IN ({})
                """.format(','.join(['?'] * len(batch_ids))), batch_ids)
                
                existing_ids = {row[0] for row in cursor.fetchall()}
                new_ids = set(batch_ids) - existing_ids
                
                log_message(f"📊 IDs existentes: {len(existing_ids)}, IDs nuevos: {len(new_ids)}")
                if existing_ids:
                    log_message(f"📋 IDs existentes: {sorted(existing_ids)}")
                
                # Construir la consulta MERGE
                columns = ', '.join(batch[0].keys())
                placeholders = ', '.join(['?' for _ in batch[0]])
                update_columns = ', '.join([f"target.{col} = source.{col}" for col in batch[0].keys() if col != 'id'])
                
                merge_sql = f"""
                MERGE INTO Candidates WITH (HOLDLOCK) AS target
                USING (VALUES ({placeholders})) AS source ({columns})
                ON target.id = source.id
                WHEN MATCHED THEN
                    UPDATE SET {update_columns}
                WHEN NOT MATCHED THEN
                    INSERT ({columns})
                    VALUES ({placeholders});
                """
                
                # Ejecutar MERGE para cada candidato en el lote
                failed_in_batch = []
                for candidate in batch:
                    try:
                        values = tuple(candidate.values())
                        cursor.execute(merge_sql, values + values)
                    except Exception as e:
                        failed_in_batch.append((candidate, str(e)))
                        log_message(f"❌ Error al procesar candidato {candidate.get('id', 'N/A')}: {str(e)}")
                
                try:
                    conn.commit()
                    
                    # Verificar que los registros se insertaron correctamente
                    cursor.execute("""
                        SELECT id FROM Candidates 
                        WHERE id IN ({})
                    """.format(','.join(['?'] * len(batch_ids))), batch_ids)
                    
                    inserted_ids = {row[0] for row in cursor.fetchall()}
                    missing_ids = set(batch_ids) - inserted_ids
                    
                    if missing_ids:
                        log_message(f"⚠️ IDs no insertados correctamente: {sorted(missing_ids)}")
                        log_message("📋 Detalle de IDs faltantes:")
                        for missing_id in sorted(missing_ids):
                            # Buscar el candidato original para obtener más información
                            candidate = next((c for c in batch if c['id'] == missing_id), {'id': missing_id})
                            error_msg = f"No se pudo verificar la inserción - ID: {missing_id}"
                            failed_in_batch.append((candidate, error_msg))
                            log_message(f"   - ID: {missing_id}")
                    
                    log_message(f"📊 Resultados del lote de {batch_size}:")
                    log_message(f"   - Esperados: {len(batch)}")
                    log_message(f"   - Insertados: {len(inserted_ids)}")
                    log_message(f"   - Fallidos: {len(failed_in_batch)}")
                    
                    if len(inserted_ids) != len(batch):
                        log_message(f"⚠️ Discrepancia detectada: {len(batch) - len(inserted_ids)} registros no se insertaron correctamente")
                        log_message("📋 Detalle de IDs faltantes:")
                        for missing_id in sorted(missing_ids):
                            log_message(f"   - ID: {missing_id}")
                        
                        if batch_size > 1:
                            # Reducir tamaño de lote y reintentar
                            next_batch_size = max(1, batch_size // 10)
                            log_message(f"🔄 Reduciendo tamaño de lote a {next_batch_size} para {len(failed_in_batch)} candidatos fallidos")
                            
                            # Procesar los fallidos con el nuevo tamaño
                            retry_failed, retry_processed = process_with_reduced_batch_size(
                                [c for c, _ in failed_in_batch], 
                                next_batch_size
                            )
                            failed_candidates.extend(retry_failed)
                            total_processed += retry_processed
                        else:
                            # Si ya estamos en tamaño 1, guardar como fallido definitivo
                            failed_candidates.extend(failed_in_batch)
                            log_message(f"❌ {len(failed_in_batch)} candidatos fallaron definitivamente")
                            log_message("📋 Detalle de IDs fallidos definitivamente:")
                            for candidate, error in failed_in_batch:
                                log_message(f"   - ID: {candidate.get('id', 'N/A')}, Error: {error}")
                
                    total_processed += len(inserted_ids)
                    
                except Exception as e:
                    conn.rollback()
                    log_message(f"❌ Error al guardar lote de {batch_size}: {str(e)}")
                    # Agregar todo el lote a los reintentos
                    if batch_size > 1:
                        next_batch_size = max(1, batch_size // 10)
                        log_message(f"🔄 Reintentando lote completo con tamaño {next_batch_size}")
                        retry_failed, retry_processed = process_with_reduced_batch_size(batch, next_batch_size)
                        failed_candidates.extend(retry_failed)
                        total_processed += retry_processed
                    else:
                        failed_candidates.extend([(c, f"Error en lote: {str(e)}") for c in batch])
                        log_message("📋 Detalle de IDs fallidos por error en lote:")
                        for candidate, error in failed_candidates:
                            log_message(f"   - ID: {candidate.get('id', 'N/A')}, Error: {error}")
            
            # Al final del proceso, mostrar un resumen de todos los fallidos
            if failed_candidates:
                log_message("\n📋 RESUMEN FINAL DE CANDIDATOS FALLIDOS:")
                for candidate, error in failed_candidates:
                    log_message(f"   - ID: {candidate.get('id', 'N/A')}, Error: {error}")
            
            return failed_candidates, total_processed

        # Procesar en lotes de 1000
        total_processed = 0
        failed_candidates = []
        
        # Procesar todos los candidatos con tamaño de lote inicial de 1000
        remaining_failed, processed = process_with_reduced_batch_size(flat_candidates, 1000)
        total_processed += processed
        failed_candidates.extend(remaining_failed)
        
        # Guardar registro de candidatos fallidos
        if failed_candidates:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            error_file = os.path.join('logs', f'failed_candidates_{timestamp}.json')
            with open(error_file, 'w', encoding='utf-8') as f:
                json.dump([{
                    'id': c.get('id', 'N/A'),
                    'data': c,
                    'reason': e
                } for c, e in failed_candidates], f, ensure_ascii=False, indent=2)
            log_message(f"⚠️ Se guardó registro de {len(failed_candidates)} candidatos fallidos en '{error_file}'")
        
        # Verificación final
        cursor.execute("SELECT COUNT(*) FROM Candidates")
        final_count = cursor.fetchone()[0]
        
        log_message(f"💾 Total de candidatos procesados exitosamente: {total_processed}")
        if failed_candidates:
            log_message(f"❌ Total de candidatos fallidos después de todos los reintentos: {len(failed_candidates)}")
            log_message("\n📋 RESUMEN FINAL DE CANDIDATOS FALLIDOS:")
            for candidate, error in failed_candidates:
                log_message(f"   - ID: {candidate.get('id', 'N/A')}, Error: {error}")
        
        log_message(f"📊 Total de registros en la base de datos: {final_count}")
        
    except Exception as e:
        log_message(f"Error al guardar en la base de datos: {e}")
        conn.rollback()
        raise  # Re-lanzar la excepción para que sea manejada en el nivel superior
    finally:
        cursor.close()
        conn.close()

async def get_candidate_details_async(session: aiohttp.ClientSession, candidate_id: str) -> Dict[str, Any]:
    """Obtiene los detalles de un candidato de forma asíncrona"""
    url = f"{CANDIDATES_API_URL}/{candidate_id}"
    retry_count = 0
    
    while retry_count < MAX_RETRIES:
        try:
            async with session.get(url, headers=HEADERS) as response:
                if response.status == 200:
                    data = await response.json()
                    candidate_data = data.get('data', {})
                    if not candidate_data:
                        return None
                    return candidate_data
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

async def get_page_by_date_async(session: aiohttp.ClientSession, page_number: int, fecha_desde: str, date_type: str) -> tuple:
    """Obtiene una página de candidatos de forma asíncrona para un tipo específico de fecha"""
    url = f"{CANDIDATES_API_URL}?page_size={PAGE_SIZE}&page={page_number}"
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
                    candidates = data.get("data", [])
                    
                    # Registrar candidatos con códigos de país inválidos
                    invalid_country_candidates = []
                    for candidate in candidates:
                        if 'country' in candidate and candidate['country']:
                            country_code = str(candidate['country']).strip()
                            if len(country_code) != 2:
                                invalid_country_candidates.append({
                                    'id': candidate.get('id', 'N/A'),
                                    'country_code': country_code,
                                    'page': page_number,
                                    'full_data': candidate
                                })
                    
                    # Si hay candidatos con códigos de país inválidos, guardarlos en un archivo
                    if invalid_country_candidates:
                        log_message(f"⚠️ Encontrados {len(invalid_country_candidates)} candidatos con códigos de país inválidos en la página {page_number}")
                        with open('invalid_country_codes.json', 'a', encoding='utf-8') as f:
                            for candidate in invalid_country_candidates:
                                f.write(json.dumps(candidate, ensure_ascii=False) + '\n')
                    
                    has_more = data.get("meta", {}).get("has_more", False)
                    total_count = data.get("meta", {}).get("total", 0)
                    
                    if page_number == 1:
                        log_message(f"📊 Total de registros para {date_type}: {total_count}")
                    
                    log_message(f"📡 Página {page_number} ({date_type}): Obtenidos {len(candidates)} candidatos")
                    return candidates, has_more
                elif response.status == 429:  # Rate limit
                    retry_after = int(response.headers.get('Retry-After', RETRY_DELAY))
                    log_message(f"Rate limit alcanzado, esperando {retry_after} segundos")
                    await asyncio.sleep(retry_after)
                    continue
                elif response.status == 422:  # Unprocessable Entity
                    error_text = await response.text()
                    log_message(f"⚠️ Error de validación en la página {page_number} ({date_type}): {error_text}")
                    # Guardar la página fallida para reintento posterior
                    failed_pages.add(page_number)
                    # Esperar un tiempo antes de reintentar
                    await asyncio.sleep(RETRY_DELAY * (attempt + 1))
                    continue
                elif response.status == 401:
                    log_message("Error de autenticación (401). Verificar API key")
                    return [], False
                elif response.status == 403:
                    log_message("Error de autorización (403). Sin permisos para acceder")
                    return [], False
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
    
    # Si llegamos aquí, significa que todos los reintentos fallaron
    if failed_pages:
        log_message(f"❌ Página {page_number} ({date_type}) falló después de {MAX_RETRIES} intentos")
        # Guardar la página fallida en un archivo para reintento posterior
        with open('Candidates_failed_pages.txt', 'a') as f:
            f.write(f"{page_number},{date_type}\n")
    
    return [], False

async def get_all_candidates_by_date_type(session: aiohttp.ClientSession, fecha_desde: str) -> List[Dict[str, Any]]:
    """Obtiene todos los candidatos para ambos tipos de fecha (created y updated)"""
    all_candidates = []
    processed_ids = set()  # Para evitar duplicados
    
    # Procesar candidatos actualizados
    log_message("\n🔄 Obteniendo candidatos actualizados...", True)
    updated_candidates = await get_all_candidates_by_date_type_specific(session, fecha_desde, "updated_after")
    for candidate in updated_candidates:
        if candidate.get('id') not in processed_ids:
            all_candidates.append(candidate)
            processed_ids.add(candidate.get('id'))
    
    # Procesar candidatos creados
    log_message("\n🔄 Obteniendo candidatos creados...", True)
    created_candidates = await get_all_candidates_by_date_type_specific(session, fecha_desde, "created_after")
    for candidate in created_candidates:
        if candidate.get('id') not in processed_ids:
            all_candidates.append(candidate)
            processed_ids.add(candidate.get('id'))
    
    log_message(f"\n📊 Total de candidatos únicos obtenidos: {len(all_candidates)}", True)
    return all_candidates

async def get_all_candidates_by_date_type_specific(session: aiohttp.ClientSession, fecha_desde: str, date_type: str) -> List[Dict[str, Any]]:
    """Obtiene todos los candidatos para un tipo específico de fecha"""
    all_candidates = []
    page = 1
    
    while True:
        candidates, has_more = await get_page_by_date_async(session, page, fecha_desde, date_type)
        if not candidates:
            break
        
        all_candidates.extend(candidates)
        if not has_more:
            break
        
        page += 1
        await asyncio.sleep(0.3)  # Pausa entre páginas
    
    return all_candidates

async def get_page_simple_async(session: aiohttp.ClientSession, page_number: int) -> tuple:
    """Obtiene una página de candidatos de forma asíncrona sin filtros de fecha"""
    url = f"{CANDIDATES_API_URL}?page_size={PAGE_SIZE}&page={page_number}"
    
    for attempt in range(MAX_RETRIES):
        try:
            log_message(f"Intento {attempt + 1}/{MAX_RETRIES} para la página {page_number}")
            async with session.get(url, headers=HEADERS) as response:
                if response.status == 200:
                    data = await response.json()
                    candidates = data.get("data", [])
                    has_more = data.get("meta", {}).get("has_more", False)
                    total_count = data.get("meta", {}).get("total", 0)
                    
                    if page_number == 1:
                        log_message(f"📡 Total de registros en la API: {total_count}", True)
                        log_message(f"📡 Tamaño de página: {PAGE_SIZE}", True)
                    
                    log_message(f"📡 Página {page_number}: Obtenidos {len(candidates)} de {total_count} candidatos", True)
                    return candidates, has_more
                elif response.status == 422:  # Unprocessable Entity
                    error_text = await response.text()
                    log_message(f"⚠️ Error de validación en la página {page_number}: {error_text}", True)
                    problematic_pages.add(page_number)
                    
                    # Esperar un tiempo antes de continuar
                    wait_time = RETRY_DELAY * (attempt + 1)
                    log_message(f"⏳ Esperando {wait_time} segundos...")
                    await asyncio.sleep(wait_time)
                    continue
                elif response.status == 429:  # Rate limit
                    retry_after = int(response.headers.get('Retry-After', RETRY_DELAY))
                    log_message(f"⚠️ Rate limit alcanzado, esperando {retry_after} segundos", True)
                    await asyncio.sleep(retry_after)
                    continue
                elif response.status == 401:
                    log_message("❌ Error de autenticación (401). Verificar API key", True)
                    return [], False
                elif response.status == 403:
                    log_message("❌ Error de autorización (403). Sin permisos para acceder", True)
                    return [], False
                else:
                    error_text = await response.text()
                    log_message(f"Error {response.status} en la página {page_number}", True)
                    log_message(f"🔍 URL: {url}", True)
                    log_message(f"🔍 Respuesta: {error_text}", True)
                    
                    if attempt == MAX_RETRIES - 1:
                        problematic_pages.add(page_number)
                    
                    if attempt < MAX_RETRIES - 1:
                        wait_time = RETRY_DELAY * (attempt + 1)
                        log_message(f"⏳ Esperando {wait_time} segundos...")
                        await asyncio.sleep(wait_time)
                        continue
                    return [], False
        except Exception as e:
            log_message(f"Error en la página {page_number}: {str(e)}")
            if attempt == MAX_RETRIES - 1:
                problematic_pages.add(page_number)
            if attempt < MAX_RETRIES - 1:
                wait_time = RETRY_DELAY * (attempt + 1)
                log_message(f"⏳ Esperando {wait_time} segundos...")
                await asyncio.sleep(wait_time)
                continue
            return [], False
    
    return [], False

async def process_failed_pages(session: aiohttp.ClientSession, failed_pages: set) -> List[Dict[str, Any]]:
    """Procesa las páginas que fallaron con tamaños más pequeños"""
    recovered_candidates = []
    error_ids = set()  # Conjunto para almacenar IDs con error
    
    # Diccionarios para llevar registro de errores
    errors_by_size = {
        '100': set(),  # Páginas que fallaron con tamaño 100
        '10': set(),   # Páginas que fallaron con tamaño 10
        '1': set()     # Páginas que fallaron con tamaño 1
    }
    
    for original_page in sorted(failed_pages):
        log_message(f"\n🔄 Procesando página fallida {original_page}...", True)
        
        # Primero intentar con tamaño de página 100
        try:
            url = f"{CANDIDATES_API_URL}?page_size=100&page={original_page}"
            async with session.get(url, headers=HEADERS) as response:
                if response.status == 200:
                    data = await response.json()
                    candidates = data.get("data", [])
                    if candidates:
                        log_message(f"✅ Página {original_page} (tamaño 100): Obtenidos {len(candidates)} candidatos", True)
                        recovered_candidates.extend(candidates)
                        continue
                elif response.status == 422:
                    log_message(f"⚠️ Error en página {original_page} con tamaño 100, intentando con tamaño 10...", True)
                    errors_by_size['100'].add(original_page)
                else:
                    log_message(f"❌ Error {response.status} en página {original_page}", True)
                    errors_by_size['100'].add(original_page)
        except Exception as e:
            log_message(f"❌ Error al procesar página {original_page}: {str(e)}", True)
            errors_by_size['100'].add(original_page)
        
        # Calcular las páginas equivalentes para tamaño 10
        start_page_10 = ((original_page - 1) * 100) // 10 + 1
        end_page_10 = start_page_10 + 9  # 10 páginas de tamaño 10
        
        log_message(f"🔄 Calculando páginas equivalentes: {start_page_10} a {end_page_10} (tamaño 10)", True)
        
        # Intentar con páginas de tamaño 10
        for page_10 in range(start_page_10, end_page_10 + 1):
            try:
                url = f"{CANDIDATES_API_URL}?page_size=10&page={page_10}"
                async with session.get(url, headers=HEADERS) as response:
                    if response.status == 200:
                        data = await response.json()
                        candidates = data.get("data", [])
                        if candidates:
                            log_message(f"✅ Página {page_10} (tamaño 10): Obtenidos {len(candidates)} candidatos", True)
                            recovered_candidates.extend(candidates)
                            continue
                    elif response.status == 422:
                        error_text = await response.text()
                        log_message(f"⚠️ Error en página {page_10} (tamaño 10): {error_text}", True)
                        errors_by_size['10'].add(page_10)
                        
                        # Intentar obtener los candidatos uno por uno
                        log_message(f"🔄 Intentando recuperar candidatos individualmente de la página {page_10}...", True)
                        
                        # Obtener los IDs de este lote
                        try:
                            error_data = json.loads(error_text)
                            if 'errors' in error_data:
                                for error in error_data['errors']:
                                    if 'details' in error and 'id' in error['details']:
                                        error_ids.add(error['details']['id'])
                        except:
                            pass
                        
                        # Calcular el offset para esta página de 10
                        offset = (page_10 - 1) * 10
                        
                        # Intentar obtener cada candidato individualmente
                        for i in range(10):
                            try:
                                single_url = f"{CANDIDATES_API_URL}?page_size=1&offset={offset + i}"
                                async with session.get(single_url, headers=HEADERS) as single_response:
                                    if single_response.status == 200:
                                        single_data = await single_response.json()
                                        single_candidate = single_data.get("data", [])
                                        if single_candidate:
                                            recovered_candidates.extend(single_candidate)
                                            log_message(f"✅ Recuperado candidato individual del offset {offset + i}", True)
                                    else:
                                        errors_by_size['1'].add(f"offset_{offset + i}")
                            except Exception as e:
                                log_message(f"❌ Error al recuperar candidato individual: {str(e)}", True)
                                errors_by_size['1'].add(f"offset_{offset + i}")
                    else:
                        log_message(f"❌ Error {response.status} en página {page_10}", True)
                        errors_by_size['10'].add(page_10)
            except Exception as e:
                log_message(f"❌ Error al procesar página {page_10}: {str(e)}", True)
                errors_by_size['10'].add(page_10)
            
            # Esperar un poco entre páginas
            await asyncio.sleep(1)
        
        # Mostrar resumen de la página procesada
        log_message(f"✅ Página {original_page}: Procesamiento completado", True)
    
    # Mostrar resumen final de errores
    log_message("\n📊 RESUMEN DE ERRORES:", True)
    log_message("=" * 50, True)
    
    if errors_by_size['100']:
        log_message(f"\n❌ Errores con tamaño de página 100:", True)
        log_message(f"Páginas: {', '.join(map(str, sorted(errors_by_size['100'])))}", True)
    
    if errors_by_size['10']:
        log_message(f"\n❌ Errores con tamaño de página 10:", True)
        log_message(f"Páginas: {', '.join(map(str, sorted(errors_by_size['10'])))}", True)
    
    if errors_by_size['1']:
        log_message(f"\n❌ Errores con tamaño de página 1:", True)
        log_message(f"Offsets: {', '.join(sorted(errors_by_size['1']))}", True)
    
    if error_ids:
        log_message(f"\n⚠️ IDs con error de validación: {sorted(error_ids)}", True)
        # Guardar IDs con error en un archivo
        with open('Candidates_error_ids.txt', 'w') as f:
            for id in sorted(error_ids):
                f.write(f"{id}\n")
    
    log_message("\n" + "=" * 50, True)
    
    return recovered_candidates

async def get_all_candidates_simple(session: aiohttp.ClientSession) -> List[Dict[str, Any]]:
    """Obtiene todos los candidatos sin filtros de fecha de forma concurrente"""
    # Primero obtenemos la primera página para saber el total
    first_page, has_more = await get_page_simple_async(session, 1)
    if not first_page:
        log_message("❌ No se pudo obtener la primera página", True)
        return []
    
    # Obtener el total_count de la respuesta de la API
    url = f"{CANDIDATES_API_URL}?page_size={PAGE_SIZE}&page=1"
    async with session.get(url, headers=HEADERS) as response:
        if response.status == 200:
            data = await response.json()
            total_count = data.get("meta", {}).get("total", 0)
        else:
            log_message(f"❌ Error al obtener el total de registros: {response.status}", True)
            return first_page
    
    if has_more and total_count > 0:
        # Calcular el número total de páginas basado en el total_count
        total_pages = (total_count + PAGE_SIZE - 1) // PAGE_SIZE
        
        # Si LIMIT_RECORDS está configurado, ajustar el total de páginas
        if LIMIT_RECORDS > 0:
            total_pages = min(total_pages, (LIMIT_RECORDS + PAGE_SIZE - 1) // PAGE_SIZE)
            log_message(f"📊 Límite de recuperación configurado: {LIMIT_RECORDS} candidatos", True)
        
        log_message(f"📊 Total de páginas a procesar: {total_pages}", True)
        
        # Procesar las páginas en lotes más grandes
        BATCH_SIZE = 500  # Procesar 500 páginas a la vez
        all_candidates = first_page
        failed_pages = set()  # Conjunto para almacenar páginas que fallaron
        processed_pages = {1}  # Incluir la primera página que ya procesamos
        
        # Primera pasada: procesar todas las páginas
        for batch_start in range(2, total_pages + 1, BATCH_SIZE):
            batch_end = min(batch_start + BATCH_SIZE, total_pages + 1)
            current_batch_pages = set(range(batch_start, batch_end))
            log_message(f"🔄 Procesando lote de páginas {batch_start} a {batch_end-1} ({(batch_end-batch_start)} páginas)", True)
            
            # Crear tareas para el lote actual
            tasks = []
            for page in range(batch_start, batch_end):
                tasks.append(get_page_simple_async(session, page))
            
            # Ejecutar las tareas del lote actual
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Procesar resultados
            successful_pages = 0
            batch_failed_pages = 0
            for i, result in enumerate(results):
                page_num = batch_start + i
                if isinstance(result, Exception) or not result[0]:  # Si hay error o no hay candidatos
                    failed_pages.add(page_num)
                    batch_failed_pages += 1
                    log_message(f"❌ Error en página {page_num}: {str(result) if isinstance(result, Exception) else 'Sin datos'}", True)
                else:
                    all_candidates.extend(result[0])
                    successful_pages += 1
                    processed_pages.add(page_num)
            
            # Verificar que todas las páginas del lote fueron procesadas
            missing_pages = current_batch_pages - processed_pages - failed_pages
            if missing_pages:
                log_message(f"⚠️ Páginas no procesadas en este lote: {sorted(missing_pages)}", True)
                failed_pages.update(missing_pages)
            
            log_message(f"📊 Lote completado: {successful_pages} páginas exitosas, {batch_failed_pages} fallidas", True)
            log_message(f"📈 Progreso total: {len(processed_pages)}/{total_pages} páginas procesadas ({len(processed_pages)/total_pages*100:.1f}%)", True)
            
            # Verificar si hemos alcanzado el límite de recuperación
            if LIMIT_RECORDS > 0 and len(all_candidates) >= LIMIT_RECORDS:
                log_message(f"✅ Límite de recuperación alcanzado: {LIMIT_RECORDS} candidatos", True)
                all_candidates = all_candidates[:LIMIT_RECORDS]
                break
            
            # Pequeña pausa entre lotes para no sobrecargar la API
            await asyncio.sleep(2)
        
        # Segunda pasada: procesar las páginas que fallaron con tamaños más pequeños
        if failed_pages and (LIMIT_RECORDS == 0 or len(all_candidates) < LIMIT_RECORDS):
            log_message(f"\n🔄 Iniciando procesamiento de {len(failed_pages)} páginas fallidas con tamaños reducidos...", True)
            recovered_candidates = await process_failed_pages(session, failed_pages)
            
            # Aplicar límite de recuperación si está configurado
            if LIMIT_RECORDS > 0:
                remaining_slots = LIMIT_RECORDS - len(all_candidates)
                if remaining_slots > 0:
                    recovered_candidates = recovered_candidates[:remaining_slots]
                    log_message(f"📊 Limitando recuperación a {remaining_slots} candidatos adicionales", True)
            
            all_candidates.extend(recovered_candidates)
            
            # Actualizar el conjunto de páginas procesadas
            processed_pages.update(failed_pages)
            
            log_message(f"📊 Recuperación completada: {len(recovered_candidates)} candidatos recuperados de páginas fallidas", True)
        
        # Verificación final y procesamiento de páginas no procesadas
        missing_pages = set(range(1, total_pages + 1)) - processed_pages
        if missing_pages and (LIMIT_RECORDS == 0 or len(all_candidates) < LIMIT_RECORDS):
            log_message(f"\n🔄 Iniciando procesamiento de {len(missing_pages)} páginas no procesadas...", True)
            recovered_candidates = await process_failed_pages(session, missing_pages)
            
            # Aplicar límite de recuperación si está configurado
            if LIMIT_RECORDS > 0:
                remaining_slots = LIMIT_RECORDS - len(all_candidates)
                if remaining_slots > 0:
                    recovered_candidates = recovered_candidates[:remaining_slots]
                    log_message(f"📊 Limitando recuperación a {remaining_slots} candidatos adicionales", True)
            
            all_candidates.extend(recovered_candidates)
            
            # Actualizar el conjunto de páginas procesadas
            processed_pages.update(missing_pages)
            
            log_message(f"📊 Recuperación de páginas no procesadas completada: {len(recovered_candidates)} candidatos recuperados", True)
        
        # Mensaje final más preciso
        if len(processed_pages) == total_pages:
            log_message(f"✅ Proceso completado: Todas las páginas procesadas exitosamente ({total_pages} páginas)", True)
        else:
            log_message(f"✅ Proceso completado: {len(processed_pages)}/{total_pages} páginas procesadas ({len(processed_pages)/total_pages*100:.1f}%)", True)
        
        return all_candidates
    else:
        return first_page

async def process_candidate_batch(session: aiohttp.ClientSession, candidates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Procesa un lote de candidatos de forma asíncrona"""
    log_message(f"\n🔄 Iniciando procesamiento de {len(candidates)} candidatos...", True)
    
    # Dividir el lote en sub-lotes más grandes para procesamiento concurrente
    sub_batch_size = BATCH_SIZE  # Usar BATCH_SIZE global
    all_results = []
    total_processed = 0
    
    # Crear un semáforo para limitar las conexiones concurrentes
    sem = asyncio.Semaphore(CONCURRENT_REQUESTS)
    
    async def process_with_semaphore(candidate_id: str) -> Dict[str, Any]:
        async with sem:
            return await get_candidate_details_async(session, candidate_id)
    
    # Procesar todos los candidatos en paralelo usando chunks
    tasks = []
    for candidate in candidates:
        tasks.append(process_with_semaphore(candidate['id']))
    
    # Procesar en chunks para mostrar progreso
    chunk_size = sub_batch_size
    for i in range(0, len(tasks), chunk_size):
        chunk = tasks[i:i + chunk_size]
        log_message(f"📦 Procesando chunk {i//chunk_size + 1}/{(len(tasks) + chunk_size - 1)//chunk_size} ({len(chunk)} candidatos)", True)
        
        # Ejecutar las tareas del chunk actual
        results = await asyncio.gather(*chunk, return_exceptions=True)
        
        # Procesar resultados
        valid_results = [r for r in results if r is not None]
        all_results.extend(valid_results)
        
        total_processed += len(valid_results)
        log_message(f"✅ Chunk completado: {len(valid_results)}/{len(chunk)} candidatos procesados", True)
        log_message(f"📈 Progreso total: {total_processed}/{len(candidates)} candidatos ({total_processed/len(candidates)*100:.1f}%)", True)
        
        # Pequeña pausa entre chunks para no sobrecargar la API
        await asyncio.sleep(0.1)  # Reducido de 0.5 a 0.1 segundos
    
    log_message(f"\n✅ Procesamiento de candidatos completado: {len(all_results)}/{len(candidates)} candidatos procesados exitosamente", True)
    return all_results

async def retry_failed_pages(session: aiohttp.ClientSession):
    """Reintenta las páginas que fallaron anteriormente"""
    try:
        with open('Candidates_failed_pages.txt', 'r') as f:
            failed_pages = f.readlines()
        
        if not failed_pages:
            return
        
        log_message(f"🔄 Reintentando {len(failed_pages)} páginas fallidas...")
        
        for page_info in failed_pages:
            page_info = page_info.strip()
            if ',' in page_info:
                page_number, date_type = page_info.split(',')
                candidates, _ = await get_page_by_date_async(session, int(page_number), None, date_type)
            else:
                page_number = int(page_info)
                candidates, _ = await get_page_simple_async(session, page_number)
            
            if candidates:
                log_message(f"✅ Página {page_number} recuperada exitosamente")
                # Guardar los candidatos recuperados
                save_to_database(candidates)
            else:
                log_message(f"❌ No se pudo recuperar la página {page_number}")
        
        # Limpiar el archivo de páginas fallidas
        open('Candidates_failed_pages.txt', 'w').close()
        
    except FileNotFoundError:
        log_message("No hay páginas fallidas para reintentar")
    except Exception as e:
        log_message(f"Error al reintentar páginas fallidas: {e}")

async def analyze_problematic_pages():
    """Analiza específicamente las páginas que están fallando para obtener más detalles"""
    if not problematic_pages:
        log_message("✅ No se detectaron páginas problemáticas en esta ejecución")
        return

    log_message(f"\n🔍 Analizando {len(problematic_pages)} páginas problemáticas detectadas:")
    async with aiohttp.ClientSession() as session:
        for page in sorted(problematic_pages):
            log_message(f"\n📄 Analizando página problemática {page}:")
            url = f"{CANDIDATES_API_URL}?page_size={PAGE_SIZE}&page={page}"
            
            try:
                async with session.get(url, headers=HEADERS) as response:
                    response_text = await response.text()
                    log_message(f"Status code: {response.status}")
                    
                    if response.status == 422:
                        try:
                            error_data = json.loads(response_text)
                            if 'errors' in error_data:
                                for error in error_data['errors']:
                                    log_message(f"Detalles del error:")
                                    log_message(f"   - Código: {error.get('code', 'N/A')}")
                                    log_message(f"   - Mensaje: {error.get('message', 'N/A')}")
                                    if 'details' in error:
                                        log_message(f"   - Detalles adicionales: {error['details']}")
                        except json.JSONDecodeError:
                            log_message(f"No se pudo parsear la respuesta de error: {response_text}")
            except Exception as e:
                log_message(f"Error al analizar la página {page}: {str(e)}")
            
            # Esperar un poco entre cada página para no sobrecargar la API
            await asyncio.sleep(2)
    
    log_message(f"\n📊 Resumen de páginas problemáticas en esta ejecución: {sorted(problematic_pages)}")

async def main():
    """Función principal que ejecuta el proceso completo"""
    # Iniciar captura de logs
    with LogCapture():
        try:
            # Registrar tiempo de inicio
            start_time = datetime.now()
            
            # Crear tablas si no existen
            log_message("\n🔄 Iniciando proceso de actualización de candidatos...", True)
            create_tables()

            # Configurar sesión HTTP con más conexiones y timeout más largo
            timeout = ClientTimeout(total=120)  # 2 minutos
            connector = TCPConnector(limit=MAX_CONNECTIONS, ttl_dns_cache=300)
            
            async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
                # Obtener última fecha de actualización
                last_update = get_last_update()
                
                if last_update:
                    log_message(f"📅 Última actualización: {last_update}", True)
                    # Obtener candidatos actualizados desde la última fecha
                    log_message("\n🔄 Obteniendo candidatos actualizados y creados...", True)
                    candidates = await get_all_candidates_by_date_type(session, last_update)
                else:
                    log_message("\n🔄 No se encontró fecha de última actualización. Obteniendo todos los candidatos...", True)
                    # Obtener todos los candidatos
                    candidates = await get_all_candidates_simple(session)
                
                if not candidates:
                    log_message("❌ No se encontraron candidatos para procesar", True)
                    return
                
                log_message(f"\n📊 Total de candidatos obtenidos: {len(candidates)}", True)
                
                # Procesar candidatos en lotes
                log_message("\n🔄 Iniciando procesamiento detallado de candidatos...", True)
                processed_candidates = await process_candidate_batch(session, candidates)
                
                if processed_candidates:
                    log_message(f"\n💾 Guardando {len(processed_candidates)} candidatos en la base de datos...", True)
                    try:
                        save_to_database(processed_candidates)
                        # Actualizar la última ejecución solo si se guardaron los datos correctamente
                        update_last_execution()
                        log_message("✅ Datos guardados correctamente en la base de datos", True)
                    except Exception as e:
                        log_message(f"❌ Error al guardar en la base de datos: {e}", True)
                        log_message("❌ No se actualizará la fecha de última ejecución debido a errores", True)
                        return  # Salir del proceso si hay errores al guardar
                else:
                    log_message("❌ No hay candidatos procesados para guardar", True)
                    return
                
                # Calcular tiempo total de ejecución
                end_time = datetime.now()
                duration = end_time - start_time
                minutes = duration.seconds // 60
                seconds = duration.seconds % 60
                
                if minutes > 0:
                    log_message(f"\n✅ Proceso de actualización completado exitosamente en {minutes} minutos y {seconds} segundos", True)
                else:
                    log_message(f"\n✅ Proceso de actualización completado exitosamente en {seconds} segundos", True)
                
        except Exception as e:
            log_message(f"❌ Error en el proceso principal: {str(e)}", True)
            raise

if __name__ == "__main__":
    asyncio.run(main())