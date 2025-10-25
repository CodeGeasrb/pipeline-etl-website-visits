from utils.utils_postprocessing import move_to_backup, remove_from_sftp, zip_compress
from prefect import task

import logging


@task(name="post-procesamiento", retries=2, retry_delay_seconds=60)
def post_processing(filepath: str, logger: logging.Logger)  -> None:
    """ Tarea para el post procesamiento, generando el backup y borrando los archivos originales """
    # Logging de inicio 
    logger.info("Iniciando etapa de post procesamiento")

    # movemos archivo descargado a backup
    logger.info("Moviendo el archivo al backup")
    move_to_backup(filepath)

    # removemos el archivo original
    logger.info("Removimiento archivo del servidor de inicio")
    remove_from_sftp()


@task(name="comprimir backup", retries=2, retry_delay_seconds=60)
def compress_backup():
    """ Tarea para comprimir todos los archivos procesados y guardarlos como backup """    
    # comprimimos mediante zip
    zip_compress()



