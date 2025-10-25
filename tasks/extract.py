import logging
from utils.utils import setup_logger, sftp_connection
from typing import Optional
from prefect import task
from pathlib import Path


# Tarea de extracción de datos
@task(name="Extracción de datos", retries=2, retry_delay_seconds=60)
def extract(filename: str, logger: logging.Logger) -> Optional[Path]:
    """ Esta tarea descarga un archivo desde el servidor de inicio a un directorio temporal en el servidor del ETL"""
    # crear logging de inicio
    logger.info("Iniciando etapa de extración")

    # validar que el directorio temporal existe o crearlo
    staging_path = Path("staging_path")
    staging_path.mkdir(parents=True, exist_ok=True)

    # definir la ruta de origen del archivo
    remote_path = f"remotedir/{filename}"

    # definir la ruta completa de descarga del archivo
    local_path = f"staging_path/{filename}"

    # descargar el archivo
    logger.info("Descargando archivo...")
    with sftp_connection() as sftp:
        sftp.get(remote_path, local_path)
        
    # loggear fin de tarea
    logger.info(f"Archivo {filename} descargado a {local_path} con éxito")
    return local_path




