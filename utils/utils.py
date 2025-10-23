from contextlib import contextmanager
from dotenv import load_dotenv

import logging
import datetime
import paramiko
import os

# Cargar variables de entorno para la configuración de la conexión SFTP y MySQL
load_dotenv(dotenv_path="config/.env")

@contextmanager
def sftp_connection():
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

""" Setup de la configuración de logger """
def setup_logger(filename: str) -> logging.Logger:
    # fecha estandarizada
    log_date = datetime.now().strftime("%d%m%y")
    # directorio del log del archivo que será procesado
    log_dir = "dir"
    log_dir.mkdir(parents=True, exist_ok=True)  # asegurar que el log existe, sino crearlo
    return logging.getLogger(filename)


""" Configuración del mysql connection string para sqlalchemy """
def mysql_connection_string() -> str:
    # leer las credenciales del servidor mysql
    host = os.getenv("HOST_MYSQL")
    port = os.getenv("PORT_MYSQL")
    user = os.getenv("USER_MYSQL")
    password = os.getenv("PASSWORD_MYSQL")
    database = os.getenv("DATABASE_MYSQL")
    # crear el connection string
    connection_string = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
    return connection_string




