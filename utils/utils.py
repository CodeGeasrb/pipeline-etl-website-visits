from contextlib import contextmanager
from dotenv import load_dotenv
from pathlib import Path
from typing import Optional

import pandas as pd
import logging
import datetime
import paramiko
import os


# Cargar variables de entorno para la configuración de la conexión SFTP y MySQL
load_dotenv(dotenv_path="config/.env")


@contextmanager
def sftp_connection():
    """ Set  up de la conexión con el servidor de inicio mediante SFTP """
    # inicializamos los recursos de conexión como None
    transport = None
    sftp = None

    # intentar conexión a servidor sftp
    try:
        # se definen las credenciales de conexión
        port = int(os.getenv("PORT_SFTP"))
        host = os.getenv("HOST_SFTP")
        # establece la conexion mediante sftp
        transport = paramiko.Transport((host, port))
        transport.connect(username=os.getenv("USER_SFTP"), password=os.getenv("PASSWORD_SFTP"))
        sftp = paramiko.SFTPClient.from_transport(transport)
        # cedemos control al with
        yield sftp

    finally:
        # cerramos la conexión si está fue establecida
        if sftp is not None:
            sftp.close()
        if transport is not None:
            transport.close()


def setup_logger(filename: str) -> logging.Logger:
    """ Setup de la configuración de logger """
    # fecha estandarizada
    log_date = datetime.now().strftime("%d%m%y")

    # directorio del log del archivo que será procesado
    log_dir = "dir"
    log_dir.mkdir(parents=True, exist_ok=True)  # asegurar que el log existe, sino crearlo

    # establecer el logger
    logger = logging.getLogger(filename)
    return logger



def mysql_connection_string() -> str:
    """ Configuración del mysql connection string para sqlalchemy """
    # leer las credenciales del servidor mysql
    host = os.getenv("HOST_MYSQL")
    port = os.getenv("PORT_MYSQL")
    user = os.getenv("USER_MYSQL")
    password = os.getenv("PASSWORD_MYSQL")
    database = os.getenv("DATABASE_MYSQL")

    # crear el connection string
    connection_string = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
    return connection_string


def validate_file_loading(filepath: Path, logger: logging.Logger) -> Optional[pd.DataFrame]:
    """ Valida que un archivo se pueda cargar como un dataframe de pandas siguiente el esque csv """
    # intentamos leer el archivo
    try:
        # leemos el archivo
        df = pd.read_csv(filepath)

        # validamos que no esté vacío   
        if df.empty:
            return None

        # devolvemos el df en caso contrario
        return df 

    # cachamos el error
    except Exception as e:
        # cualquier error
        logger.error(f"Fallo al cargar el archivo. Error: {str(e)}")
        # no devolvemos nada
        return None
        



