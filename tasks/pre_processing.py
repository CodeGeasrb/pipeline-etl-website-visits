from utils.utils import sftp_connection
from prefect import task
from typing import List

import os


@task(name="Enlistar archivos nuevos", retries=2, retry_delay_seconds=60)
def list_files() -> List[str]:
    """ Función que enlista la cantidad de archivos a procesar """
    # conectamos con servidor 
    with sftp_connection() as sftp:
        # buscamos el directorio de datos
        sftp.chdir(os.getenv("DIR_SFTP"))

        # enlistamos archivos
        files = [
            file for file in sftp.listdir()
            if file.startswith("report_") and file.endswith(".txt")
        ]
    return files



    