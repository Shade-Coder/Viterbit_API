import subprocess
import sys
import time
from datetime import datetime
import os
import pyodbc
import locale
from io import StringIO
from dotenv import load_dotenv

# Cargar variables de entorno desde .env
load_dotenv()

# Configurar codificación para Windows
if sys.platform == 'win32':
    # Intentar configurar la codificación de la consola a UTF-8
    try:
        import ctypes
        kernel32 = ctypes.windll.kernel32
        kernel32.SetConsoleOutputCP(65001)
        kernel32.SetConsoleCP(65001)
    except Exception:
        pass
    
    # Configurar la codificación por defecto
    if sys.stdout.encoding != 'utf-8':
        sys.stdout.reconfigure(encoding='utf-8')
    if sys.stderr.encoding != 'utf-8':
        sys.stderr.reconfigure(encoding='utf-8')

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

def create_database():
    """
    Crea la base de datos ViterbitDB si no existe.
    
    Esta función:
    1. Se conecta al servidor SQL Server sin especificar una base de datos
    2. Verifica si existe la base de datos ViterbitDB
    3. Si no existe, la crea
    4. Maneja errores de conexión y creación
    
    Returns:
        bool: True si la base de datos existe o se creó exitosamente, False en caso de error
    """
    try:
        # Intentar diferentes drivers de SQL Server
        drivers = [
            '{ODBC Driver 17 for SQL Server}',
            '{SQL Server}',
            '{SQL Server Native Client 11.0}',
            '{SQL Server Native Client 10.0}'
        ]
        
        conn = None
        for driver in drivers:
            try:
                conn_str = (
                    f'DRIVER={driver};'
            'SERVER=localhost;'
            'Trusted_Connection=yes;'
        )
                conn = pyodbc.connect(conn_str, autocommit=True)  # Habilitar autocommit
                break
            except pyodbc.Error:
                continue
        
        if not conn:
            print("[ERROR] No se pudo conectar a SQL Server. Verifica que:")
            print("1. SQL Server esté instalado y ejecutándose")
            print("2. El servicio SQL Server esté activo")
            print("3. Tengas los drivers de SQL Server instalados")
            return False
            
        cursor = conn.cursor()
        
        # Verificar si la base de datos existe
        cursor.execute("""
            SELECT database_id 
            FROM sys.databases 
            WHERE name = 'ViterbitDB'
        """)
        
        if not cursor.fetchone():
            # Crear la base de datos
            cursor.execute("CREATE DATABASE ViterbitDB")
            print("[OK] Base de datos ViterbitDB creada exitosamente")
        
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        print(f"[ERROR] Error al crear/verificar la base de datos: {e}")
        print("\nPor favor, verifica que:")
        print("1. SQL Server esté instalado y ejecutándose")
        print("2. El servicio SQL Server esté activo")
        print("3. Tengas los drivers de SQL Server instalados")
        print("4. Tengas permisos de administrador en SQL Server")
        return False

def create_ultima_actualizacion_table():
    """
    Crea la tabla Ultima_Actualizacion si no existe.
    
    Esta función:
    1. Verifica si existe la tabla Ultima_Actualizacion
    2. Si no existe, la crea con los campos:
       - nombre_script (VARCHAR(100)): Nombre del script ejecutado
       - ultima_actualizacion (DATETIME): Fecha y hora de la última ejecución
    3. Maneja errores de conexión y creación
    
    Returns:
        bool: True si la tabla existe o se creó exitosamente, False en caso de error
    """
    conn = get_db_connection()
    if not conn:
        return False
    
    cursor = conn.cursor()
    try:
        # Verificar si la tabla existe
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
            print("[OK] Tabla Ultima_Actualizacion creada exitosamente")
        
        conn.commit()
        return True
    except Exception as e:
        print(f"[ERROR] Error al crear/verificar la tabla Ultima_Actualizacion: {e}")
        return False
    finally:
        cursor.close()
        conn.close()

def get_db_connection():
    """
    Establece conexión con la base de datos SQL Server.
    
    Esta función:
    1. Verifica y crea la base de datos si no existe
    2. Establece conexión con la base de datos ViterbitDB
    3. Maneja errores de conexión
    
    Returns:
        pyodbc.Connection: Objeto de conexión a la base de datos o None en caso de error
    """
    try:
        # Primero verificar y crear la base de datos si no existe
        if not create_database():
            return None
            
        # Intentar diferentes drivers de SQL Server
        drivers = [
            '{ODBC Driver 17 for SQL Server}',
            '{SQL Server}',
            '{SQL Server Native Client 11.0}',
            '{SQL Server Native Client 10.0}'
        ]
        
        conn = None
        for driver in drivers:
            try:
                conn_str = (
                    f'DRIVER={driver};'
                    'SERVER=localhost;'
                    'DATABASE=ViterbitDB;'
                    'Trusted_Connection=yes;'
                )
                conn = pyodbc.connect(conn_str)
                return conn
            except pyodbc.Error:
                continue
        
        if not conn:
            print("[ERROR] No se pudo conectar a la base de datos ViterbitDB. Verifica que:")
            print("1. SQL Server esté instalado y ejecutándose")
            print("2. El servicio SQL Server esté activo")
            print("3. Tengas los drivers de SQL Server instalados")
            print("4. La base de datos ViterbitDB exista")
            return None
            
    except Exception as e:
        print(f"[ERROR] Error al conectar con la base de datos: {e}")
        return None

def update_last_execution():
    """
    Registra la ejecución actual en la tabla Ultima_Actualizacion.
    
    Esta función:
    1. Verifica y crea la tabla Ultima_Actualizacion si no existe
    2. Actualiza o inserta el registro de la última ejecución para master_script.py
    3. Maneja errores de conexión y actualización
    4. Proporciona feedback sobre el resultado de la operación
    """
    # Primero verificar y crear la tabla si no existe
    if not create_ultima_actualizacion_table():
        return
        
    conn = get_db_connection()
    if not conn:
        print("[ERROR] No se pudo conectar a la base de datos para actualizar última ejecución")
        return
    
    cursor = conn.cursor()
    try:
        fecha_actual = datetime.now()
        
        # Intentar actualizar primero
        cursor.execute("""
        UPDATE Ultima_Actualizacion 
        SET ultima_actualizacion = ?
        WHERE nombre_script = ?
        """, (fecha_actual, 'master_script.py'))
        
        # Si no se actualizó ninguna fila, insertar
        if cursor.rowcount == 0:
            cursor.execute("""
            INSERT INTO Ultima_Actualizacion (nombre_script, ultima_actualizacion)
            VALUES (?, ?)
            """, ('master_script.py', fecha_actual))
        
        conn.commit()
        print(f"[OK] Actualizada última ejecución: {fecha_actual.strftime('%Y-%m-%d %H:%M:%S')}")
    except Exception as e:
        print(f"[ERROR] Error al actualizar última ejecución: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

def ejecutar_script(nombre_script):
    """
    Ejecuta un script de Python y maneja los posibles errores.
    
    Esta función:
    1. Ejecuta el script especificado usando subprocess
    2. Captura y registra la salida estándar y de error
    3. Maneja errores de codificación de caracteres
    4. Registra la ejecución en el archivo de log
    5. Proporciona feedback sobre el resultado de la ejecución
    
    Args:
        nombre_script (str): Nombre del script a ejecutar
    
    Returns:
        bool: True si el script se ejecutó exitosamente, False en caso de error
    """
    log_message = f"\n{'='*80}\n"
    log_message += f"Ejecutando {nombre_script} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
    log_message += f"{'='*80}\n"
    
    print(log_message)
    
    try:
        # Configurar el proceso para usar UTF-8
        env = os.environ.copy()
        env["PYTHONIOENCODING"] = "utf-8"
        
        resultado = subprocess.run(
            ['py', nombre_script], 
            capture_output=True, 
            encoding='utf-8',
            errors='replace',
            env=env
        )
        
        # Imprimir salida de forma segura
        if resultado.stdout:
            print(resultado.stdout)
        
        if resultado.returncode != 0:
            error_msg = f"[ERROR] Error al ejecutar {nombre_script}:\n"
            print(error_msg)
            if resultado.stderr:
                print(resultado.stderr)
            return False
        return True
    except Exception as e:
        error_msg = f"[ERROR] Error al ejecutar {nombre_script}: {str(e)}\n"
        print(error_msg)
        return False

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
        
        self.log_file = os.path.join('logs', f'Master_script_execution_log_{self.timestamp}.txt')
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
                if file.startswith('Master_script_execution_log_') and file.endswith('.txt'):
                    full_path = os.path.join('logs', file)
                    log_files.append((full_path, os.path.getmtime(full_path)))
            
            # Ordenar por fecha de modificación (más reciente primero)
            log_files.sort(key=lambda x: x[1], reverse=True)
            
            # Eliminar logs antiguos si exceden el máximo
            if len(log_files) >= self.max_logs:
                for file_path, _ in log_files[self.max_logs:]:
                    try:
                        os.remove(file_path)
                        print(f"🗑️ Eliminado log antiguo: {os.path.basename(file_path)}")
                    except Exception as e:
                        print(f"⚠️ Error al eliminar log antiguo {file_path}: {str(e)}")
        except Exception as e:
            print(f"⚠️ Error al limpiar logs antiguos: {str(e)}")

    def _get_last_log_content(self):
        """Obtiene el contenido del último archivo de log si existe.
        
        Returns:
            str: Contenido del último archivo de log, o None si no existe.
        """
        try:
            log_files = []
            for file in os.listdir('logs'):
                if file.startswith('Master_script_execution_log_') and file.endswith('.txt'):
                    full_path = os.path.join('logs', file)
                    log_files.append((full_path, os.path.getmtime(full_path)))
            
            if log_files:
                # Ordenar por fecha de modificación (más reciente primero)
                log_files.sort(key=lambda x: x[1], reverse=True)
                last_log_path = log_files[0][0]
                
                with open(last_log_path, 'r', encoding='utf-8') as f:
                    return f.read()
        except Exception as e:
            print(f"⚠️ Error al leer último log: {str(e)}")
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

def check_api_key():
    """
    Verifica si la API key existe y es válida en el archivo .env.
    
    Returns:
        bool: True si la API key existe y es válida, False en caso contrario
    """
    try:
        # Obtener la API key desde las variables de entorno
        api_key = os.getenv('VITERBIT_API_KEY')
        
        # Verificar si la API key está vacía o es None
        if not api_key or api_key.strip() == "":
            print("\n[ERROR] La API key está vacía o no está configurada en el archivo .env")
            print("Por favor, configura una API key válida en el archivo .env")
            return False
            
        return True
    except Exception as e:
        print(f"\n[ERROR] Error al verificar la API key: {str(e)}")
        print("Asegúrate de que el archivo .env existe y contiene la variable API_KEY")
        return False

def main():
    """
    Función principal que orquesta la ejecución de todos los scripts.
    
    Esta función:
    1. Verifica la API key
    2. Crea la base de datos y tablas necesarias
    3. Define el orden de ejecución de los scripts
    4. Crea un archivo de log con timestamp
    5. Ejecuta cada script en secuencia
    6. Registra éxitos y fallos
    7. Genera un resumen de la ejecución
    8. Actualiza la última fecha de ejecución en la base de datos SOLO si todos los scripts
       se ejecutaron correctamente
    9. Maneja errores y proporciona feedback detallado
    """
    # Verificar API key primero
    if not check_api_key():
        print("\n[ERROR] No se puede continuar sin una API key válida.")
        return
        
    # Lista de scripts a ejecutar en orden
    scripts = [
        "users.py",
        "jobs.py",
        "candidates.py",
        "candidatures.py"
    ]
    
    # Iniciar captura de logs
    with LogCapture():
        print("\nInicializando base de datos...")
        
        # Crear base de datos y tabla Ultima_Actualizacion
        if not create_database():
            print("[ERROR] No se pudo crear/verificar la base de datos. Abortando ejecución.")
            return
            
        if not create_ultima_actualizacion_table():
            print("[ERROR] No se pudo crear/verificar la tabla Ultima_Actualizacion. Abortando ejecución.")
            return
            
        print("[OK] Base de datos y tablas inicializadas correctamente\n")
        
        # Contador de éxitos y fallos
        exitos = 0
        fallos = []
        
        # Tiempo inicial
        tiempo_inicio = time.time()
        
        inicio_msg = f"Iniciando proceso de actualización - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        inicio_msg += "Se ejecutarán los scripts en modo incremental\n\n"
        print(inicio_msg)
        
        # Ejecutar cada script
        for script in scripts:
            if ejecutar_script(script):
                exitos += 1
                success_msg = f"\n[OK] {script} completado exitosamente\n"
                print(success_msg)
            else:
                fallos.append(script)
                fail_msg = f"\n[ERROR] {script} falló en su ejecución\n"
                print(fail_msg)
            
            # Pequeña pausa entre scripts
            time.sleep(2)
        
        # Tiempo total
        tiempo_total = time.time() - tiempo_inicio
        minutos = int(tiempo_total // 60)
        segundos = int(tiempo_total % 60)
        
        # Resumen final
        resumen = "\n" + "="*80 + "\n"
        resumen += "RESUMEN DE EJECUCIÓN\n"
        resumen += "="*80 + "\n"
        resumen += f"Scripts ejecutados: {len(scripts)}\n"
        resumen += f"Scripts exitosos: {exitos}\n"
        resumen += f"Scripts fallidos: {len(fallos)}\n"
        
        if fallos:
            resumen += "\nScripts que fallaron:\n"
            for script in fallos:
                resumen += f"- {script}\n"
        
        resumen += f"\nTiempo total de ejecución: {minutos} minutos y {segundos} segundos\n"
        resumen += "="*80 + "\n"
        
        print(resumen)
        
        # Actualizar última ejecución solo si todos los scripts fueron exitosos
        if not fallos:
            update_last_execution()
        else:
            print("\n[ADVERTENCIA] No se actualizará la última fecha de ejecución debido a errores en algunos scripts.")

if __name__ == "__main__":
    main()