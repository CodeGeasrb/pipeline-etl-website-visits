from contextlib import contextmanager
from dotenv import load_dotenv
from pathlib import Path
from typing import Optional, Set, Tuple


import pandas as pd
import numpy as np
import logging
import datetime
import paramiko
import time
import os


# Cargar variables de entorno para la configuración de la conexión SFTP y MySQL
load_dotenv(dotenv_path="config/.env")


# Columnas esperadas por archivo
VALID_COLUMNS = [
        'email', 'fecha_visita', 'jyv', 'Badmail', 'Baja', 
        'Fecha_envio', 'Fecha_open', 'Opens', 'Opens_virales',
        'Fecha_click', 'Clicks', 'Clicks_virales', 'Links', 
        'IPs', 'Navegadores', 'Plataformas'
]


# Nombre de columnas de fechas en datos
DATE_COLUMNS = [
        "Fecha envio",
        "Fecha open",
        "Fecha click"
]


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
    """ Función que valida que un archivo se pueda cargar como un dataframe de pandas y que no esté vacío"""
    # leemos el archivo
    df = pd.read_csv(filepath)

    # validamos que no esté vacío   
    if df.empty:
        logger.warning("Alerta: El archivo se encuentra vacío")
    
    return df


def validate_file_layout(file_df: pd.DataFrame,  logger: logging.Logger) -> bool:
    """ Función que valida que un archivo cumpla con el formato de layout esperado """
    # creamos una copia del dataframe original por buenas prácticas
    file_copy = file_df.copy()

    # convertimos el conjunto de columnas del archivo en un set
    file_columns_set = set(file_copy.columns)

    # validación de columnas esperadas
    logger.info("Validando columnas esperadas")
    valid_columns = set(VALID_COLUMNS)
    missing_columns = valid_columns - file_columns_set
    if missing_columns:
        logger.error("Fallo al cargar el archivo. Error: El layout no concuerda con el esperado")
        return False
    
    # validando que existan columnas extra
    logger.info("Validando presencia de columnas adicionales")
    extra_columns = file_columns_set - valid_columns
    if extra_columns:
        # se lanza una advertencia
        logger.warning("Advertencia: Se han encontrado columnas adicionales en el archivo")
    return True


def validate_data_quality(file_df: pd.DataFrame, logger: logging.Logger) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Función que valida la calidad de los datos con base al mail y los formatos de fecha"""
    # creamos una copia del dataframe original por buenas prácticas
    file_df_copy = file_df.copy()
    
    # validación del formato del mail
    logger.info("Validando formato de email")
    email_pattern = r"^[a-zA-Z0-9][a-zA-Z0-9._%+-]*@[a-zA-Z0-9]([a-zA-Z0-9-]*[a-zA-Z0-9])?(\.[a-zA-Z0-9]([a-zA-Z0-9-]*[a-zA-Z0-9])?)*\.[a-zA-Z]{2,}$"
    file_df_copy["valid_email"] = (
        file_df_copy["email"].notna() &  # no es nulo
        file_df_copy["email"].astype(str).str.strip().ne("") &  # no está vacío
        file_df_copy["email"].astype(str).str.strip().str.match(email_pattern, na=False)
    )
    
    # validación de formato de fechas
    logger.info("Validando formato de fechas")
    date_pattern = r"^(0[1-9]|[12][0-9]|3[01])/(0[1-9]|1[0-2])/\d{4}\s([01][0-9]|2[0-3]):[0-5][0-9]$"
    for column_name in DATE_COLUMNS:
        file_df_copy[f"valid_{column_name}"] = (
            file_df_copy[column_name].isna() |  # null o nan es válido en este caso
            (
                file_df_copy[column_name].notna() & 
                file_df_copy[column_name].astype(str).str.strip().ne("") &
                file_df_copy[column_name].astype(str).str.strip().str.match(date_pattern, na=False)
            )
        )
    
    file_df_copy["valid_dates"] = file_df_copy[[f"valid_{c}" for c in DATE_COLUMNS if c in file_df_copy.columns]].all(axis=1)
    
    # separamos los dataframes en registros validos/no válidos
    file_df_copy["is_valid"] = file_df_copy["valid_email"] & file_df_copy["valid_dates"]
    file_df_copy_ok = file_df_copy[file_df_copy["is_valid"]].copy()
    file_df_copy_err = file_df_copy[~file_df_copy["is_valid"]].copy()
    
    # agregar el tipo de error en caso de fallo de alguna validación
    if len(file_df_copy_err) > 0:
        # Lista para almacenar los registros expandidos con un error por fila
        expanded_errors = []
        
        for idx, row in file_df_copy_err.iterrows():
            # Identificar qué validaciones fallaron
            errors_found = []
            
            # Verificar email
            if not row["valid_email"]:
                errors_found.append("email")
            
            # Verificar cada fecha
            if not row.get("valid_Fecha envio", True):
                errors_found.append("fecha envio")
            
            if not row.get("valid_Fecha open", True):
                errors_found.append("fecha open")
            
            if not row.get("valid_Fecha click", True):
                errors_found.append("fecha click")
            
            # Crear un registro por cada error encontrado
            for error_type in errors_found:
                error_row = row.copy()
                error_row["tipoError"] = error_type
                expanded_errors.append(error_row)
        
        # Convertir la lista de errores a DataFrame
        file_df_copy_err = pd.DataFrame(expanded_errors)
        
        # Limpiar columnas auxiliares de validación
        validation_cols = ["valid_email", "valid_dates", "is_valid"] + \
                         [f"valid_{c}" for c in DATE_COLUMNS if f"valid_{c}" in file_df_copy_err.columns]
        file_df_copy_err = file_df_copy_err.drop(columns=validation_cols, errors='ignore')
    
    # Limpiar columnas auxiliares del DataFrame de registros válidos
    validation_cols = ["valid_email", "valid_dates", "is_valid"] + \
                     [f"valid_{c}" for c in DATE_COLUMNS if f"valid_{c}" in file_df_copy_ok.columns]
    file_df_copy_ok = file_df_copy_ok.drop(columns=validation_cols, errors='ignore')
    
    # logging de resultados de validación
    total_records = len(file_df)
    valid_records_count = len(file_df_copy_ok)
    invalid_records_count = len(file_df_copy_err)
    unique_error_records = len(file_df_copy[~file_df_copy["is_valid"]])  # registros únicos con error
    
    logger.info(f"Total de registros: {total_records}")
    logger.info(f"Registros válidos: {valid_records_count} ({valid_records_count/total_records*100:.2f}%)")
    logger.info(f"Registros con errores (únicos): {unique_error_records} ({unique_error_records/total_records*100:.2f}%)")
    logger.info(f"Total de errores desglosados: {invalid_records_count}")
    return file_df_copy_ok, file_df_copy_err


def prepare_data(filename: str, file_ok_df: pd.DataFrame, file_err_df: pd.DataFrame, logger: logging.Logger) -> Tuple(pd.DataFrame, pd.DataFrame):
    """ Función para hacer las correcciones necesarias para dejar listas las tablas, previo a la carga """
    # normalizamos los valores null/nan en los datos
    logger.info("Normalizando elementos nulos")
    file_ok_df = file_ok_df.replace("-", np.nan)
    file_err_df =  file_err_df.replace("-", np.nan)

    # preparando la tabla de estadísticas
    logger.info("Preparando tabla 'estadísticas'")
    stats_df = file_ok_df.copy()

    # preparando la tabla de errores
    if not file_err_df.empty:
        logger.info("Preparando tabla 'errores'")
        file_err_df["nombreArchivo"] = filename
        errors_df = file_err_df[["nombreArchivo", "email", "tipoError"]].copy()
        return stats_df, errors_df 
    return stats_df, None





    
        

