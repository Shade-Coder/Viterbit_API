# Viterbit API Integration

Este proyecto contiene scripts para interactuar con la API de Viterbit y gestionar datos de candidaturas, trabajos, candidatos y usuarios. Los scripts realizan actualizaciones incrementales y mantienen un registro de las últimas ejecuciones en una base de datos SQL Server.

## Estructura del Proyecto

- `users.py`: Maneja la obtención y procesamiento de datos de usuarios
- `master_script.py`: Script principal que ejecuta todos los demás scripts en secuencia y gestiona el registro de ejecuciones
- `jobs.py`: Gestiona la obtención y procesamiento de datos de trabajos
- `candidates.py`: Maneja la obtención y procesamiento de datos de candidatos
- `candidatures.py`: Gestiona la obtención y procesamiento de datos de candidaturas
- `logs/`: Directorio donde se almacenan los archivos de log de ejecución

## Requisitos del Sistema

- Python 3.8 o superior
- SQL Server (local o remoto)
- Acceso a la API de Viterbit
- Conexión a Internet
- ODBC Driver 17 for SQL Server
- Terminal con soporte UTF-8 (para Windows, usar Windows Terminal o PowerShell con codificación UTF-8)

## Dependencias

El proyecto utiliza las siguientes bibliotecas (especificadas en `requirements.txt`):
```
requests==2.31.0
python-dotenv==1.0.0
aiohttp==3.9.1
pyodbc==5.0.1
typing-extensions==4.9.0
asyncio==3.4.3
```

## Configuración

1. Clona el repositorio:
   ```bash
   git clone [URL_DEL_REPOSITORIO]
   cd Viterbit-master
   ```

2. Instala las dependencias:
   ```bash
   pip install -r requirements.txt
   ```

3. Configura las variables de entorno:
   - Crea un archivo `.env` en el directorio raíz
   - Añade las siguientes variables:
     ```
     VITERBIT_API_KEY=tu_api_key
     VITERBIT_API_URL=https://api.viterbit.com/v1
     SQL_SERVER=tu_servidor_sql
     SQL_DATABASE=ViterbitDB
     ```

4. Configura la conexión a SQL Server:
   - Asegúrate de tener instalado el driver ODBC de SQL Server
   - Verifica que el servidor SQL esté accesible
   - La base de datos se creará automáticamente si no existe

5. Configuración de codificación (Windows):
   - Para PowerShell:
     ```powershell
     $OutputEncoding = [System.Text.Encoding]::UTF8
     [Console]::OutputEncoding = [System.Text.Encoding]::UTF8
     ```
   - Para CMD:
     ```cmd
     chcp 65001
     ```
   - Alternativamente, usa Windows Terminal que soporta UTF-8 por defecto

## Uso

### Ejecución Completa

Para ejecutar todos los scripts en secuencia:
```bash
python master_script.py
```

El script principal:
- Ejecuta los scripts en el orden correcto (users.py, jobs.py, candidates.py, candidatures.py)
- Genera logs detallados con timestamp
- Registra la última ejecución exitosa en la base de datos
- Maneja errores y proporciona resúmenes de ejecución
- Solo actualiza la última ejecución si todos los scripts se completan exitosamente

### Ejecución Individual

Para ejecutar un script específico:
```bash
python jobs.py
python candidates.py
python candidatures.py
python users.py
```

## Características

- **Actualización Incremental**: Los scripts solo procesan datos nuevos o modificados desde la última ejecución
- **Manejo de Errores**: Implementa reintentos (máximo 5) y manejo de excepciones
- **Logging**: Genera logs detallados de cada ejecución con timestamps
- **Base de Datos**: Almacena el historial de ejecuciones y los datos procesados
- **Procesamiento Asíncrono**: Utiliza asyncio para operaciones concurrentes
- **Validación de Datos**: Verifica la integridad de los datos antes de guardarlos
- **Rate Limiting**: Maneja automáticamente los límites de la API con reintentos
- **Progreso en Tiempo Real**: Muestra el progreso de procesamiento de registros

## Estructura de la Base de Datos

El proyecto utiliza las siguientes tablas:
- `Ultima_Actualizacion`: Registra la última ejecución exitosa de cada script
  - nombre_script (VARCHAR(100)): Nombre del script ejecutado
  - ultima_actualizacion (DATETIME): Fecha y hora de la última ejecución
- `Jobs`: Almacena información de trabajos
- `Candidates`: Almacena información de candidatos
- `Candidatures`: Almacena información de candidaturas
- `Users`: Almacena información de usuarios

## Logs

Los logs se almacenan en el directorio `logs/` con el siguiente formato:
- `Master_script_execution_log_YYYYMMDD_HHMMSS.txt`: Log del script principal
- `[Script]_execution_log_YYYYMMDD_HHMMSS.txt`: Logs de scripts individuales

Cada log incluye:
- Timestamp de inicio y fin
- Progreso de procesamiento
- Errores y advertencias
- Resumen de ejecución
- Estadísticas de procesamiento

## Notas Importantes

- Los scripts están diseñados para ejecutarse en secuencia
- Se recomienda ejecutar primero `master_script.py`
- La base de datos se crea automáticamente si no existe
- Los logs se mantienen para referencia y depuración
- Se implementa manejo de errores y reintentos automáticos
- El sistema maneja rate limits de la API automáticamente
- Los scripts procesan los datos en lotes para optimizar el rendimiento

## Solución de Problemas

### Problemas de Codificación

Si encuentras errores como:
```
UnicodeEncodeError: 'charmap' codec can't encode character
```

Sigue estos pasos:

1. En Windows:
   - Usa Windows Terminal en lugar de CMD o PowerShell estándar
   - O configura la codificación UTF-8 en PowerShell:
     ```powershell
     $OutputEncoding = [System.Text.Encoding]::UTF8
     [Console]::OutputEncoding = [System.Text.Encoding]::UTF8
     ```

2. En Linux/Mac:
   - Asegúrate de que tu terminal esté configurada para usar UTF-8
   - Ejecuta: `export LANG=en_US.UTF-8`

3. Alternativa:
   - Si los problemas persisten, puedes modificar los scripts para usar caracteres ASCII en lugar de emojis

### Otros Problemas Comunes

1. Conexión a la Base de Datos:
   - Verifica que el servidor SQL esté accesible
   - Confirma que el driver ODBC está instalado correctamente
   - Comprueba las credenciales en el archivo .env

2. API de Viterbit:
   - Verifica que la API key sea válida
   - Comprueba la conectividad a la API
   - Revisa los límites de rate limiting

## Soporte

Para reportar problemas o solicitar ayuda:
1. Revisa los logs de ejecución en el directorio `logs/`
2. Verifica la conexión a la base de datos y el driver ODBC
3. Confirma que las variables de entorno están configuradas correctamente
4. Asegúrate de tener acceso a la API de Viterbit
5. Verifica que el servidor SQL Server esté accesible
6. Comprueba la configuración de codificación de tu terminal 