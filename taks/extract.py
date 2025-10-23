from utils.utils import setup_logger, sftp_connection
from prefect import task
from pathlib import Path


# Tarea de extracción de datos
@task(name="Extracción de datos", retries=2, retry_delay_seconds=60)
def extraction(filename: str) -> Path:
    """Descarga un archivo desde el servidor de inicio a un directorio temporal en el servidor del ETL"""
    # inicializar un archivo log
    logger = setup_logger(filename)
    # crear logging de inicio
    logger.info("Iniciando etapa de extración...")
    # validar que el directorio temporal existe o crearlo
    "staging_path".mkdir(parents=True, exist_ok=True)
    # definir la ruta de origen del archivo
    remote_path = f"remotedir/{filename}"
    # definir la ruta completa de descarga del archivo
    local_path = f"staging_path/{filename}"
    # descargar el archivo
    with sftp_connection() as sftp:
        sftp.get(remote_path, local_path)
    # loggear fin de tarea
    logger.info(f"Archivo {filename} descargado a {local_path} con éxito")
    return local_path










