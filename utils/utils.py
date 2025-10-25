from contextlib import contextmanager
from sqlite3 import Connection
from dotenv import load_dotenv
from pathlib import Path
from typing import Optional, Set, Tuple, List
from prefect import task
from sqlalchemy import create_engine, text

import pandas as pd
import numpy as np
import zipfile
import logging
import datetime
import paramiko
import os


# Cargar variables de entorno para la configuración de la conexión SFTP y MySQL
load_dotenv(dotenv_path="config/.env")


# Columnas esperadas por archivo
VALID_COLUMNS = [
        'email','jyv', 'Badmail', 'Baja', 
        'Fecha envio', 'Fecha open', 'Opens', 'Opens virales',
        'Fecha click', 'Clicks', 'Clicks virales', 'Links', 
        'IPs', 'Navegadores', 'Plataformas'
]


# Nombre de columnas de fechas en datos
DATE_COLUMNS = [
        "Fecha envio",
        "Fecha open",
        "Fecha click"
]

COLUMNS_TO_MAP = {
    "email": "email",
    "jyv": "jyv",
    "Badmail": "badMail",
    "Baja": "baja",
    "Fecha envio": "fechaEnvio",
    "Fecha open": "fechaOpen",
    "Opens": "opens",
    "Opens virales": "opensVirales",
    "Fecha click": "fechaClick",
    "Clicks": "clicks",
    "Clicks virales": "clicksVirales",
    "Links": "links",
    "IPs": "ips",
    "Navegadores": "navegadores",
    "Plataformas": "plataformas"
}


COLUMNS_DATA_TYPES = {
    "email": "str",
    "jyv": "str",
    "Badmail": "str",
    "Baja": "str",
    "Fecha envio": "datetime",
    "Fecha open": "datetime",
    "Opens": "int",
    "Opens virales": "int",
    "Fecha click": "datetime",
    "Clicks": "int",
    "Clicks virales": "int",
    "Links": "str",
    "IPs": "str",
    "Navegadores": "str",
    "Plataformas": "str"
}


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
    log_date = datetime.now().strftime('%d%m%y')

    # directorio del log del archivo que será procesado
    log_dir = os.getenv("DIR_LOGS") / log_date
    log_dir.mkdir(parents=True, exist_ok=True)  # asegurar que el directorio donde se guardan estos logs existe, sino crearlo

    # configuración del logger
    logger = logging.getLogger(filename)
    logger.setLevel(logging.INFO)   # nivel de debug

    # establecer el logger
    handler = logging.FileHandler(log_dir / f"{filename}.log")
    logger.addHandler(handler)
    
    return logger


def create_mysql_connection_url() -> str:
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
                errors_found.append("Email")
            
            # Verificar cada fecha
            if not row.get("valid_Fecha envio", True):
                errors_found.append("Fecha envio")
            
            if not row.get("valid_Fecha open", True):
                errors_found.append("Fecha open")
            
            if not row.get("valid_Fecha click", True):
                errors_found.append("Fecha click")
            
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
    file_ok_df = file_ok_df.replace(["-", "0", 0], np.nan)
    file_err_df =  file_err_df.replace(["-", "0", 0], np.nan)

    # renombramos columnas
    file_ok_df.rename(columns=COLUMNS_TO_MAP, axis=1, inplace=True)
    file_err_df.rename(columns=COLUMNS_TO_MAP, axis=1, inplace=True)

    # inicializamos tablas
    stats_df = pd.DataFrame()
    visitors_df = pd.DataFrame()
    errors_df = pd.DataFrame()

    # aseguramos el tipo de datos
    for column, datatype, in COLUMNS_DATA_TYPES.items():
        if datatype == "str":
            file_ok_df[column] = file_ok_df[column].astype(str).str.strip()
            file_err_df[column] = file_err_df[column].astype(str).str.strip()
        
        elif datatype == "datetime":
            file_ok_df[column] = pd.to_datetime(file_ok_df[column], errors="coerce")
            file_err_df[column] = pd.to_datetime(file_err_df[column], errors="coerce")
        
        elif datatype == "int":
            file_ok_df[column] = pd.to_numeric(file_ok_df[column], errors="coerce").astype(int)
            file_err_df[column] = pd.to_numeric(file_err_df[column], errors="coerce").astype(int)

    # preparando la tabla de estadísticas
    logger.info("Preparando tabla 'estadísticas'")
    stats_df = file_ok_df.copy()    # copiamos directamente el dataframe de registros válidos

    # creamos una tabla de visitantes temporal (basado en registros de este archivo)
    visitors_df = file_ok_df.groupby("email").agg(
        visitasTotales=("email", "count"),
        visitasAnioActual=("email", "count"),
        visitasMesActual=("email", "count")
    )
    visitors_df["fechaPrimeraVisita"] = datetime.date.today().strftime('%d%m%y')
    visitors_df["fechaUltimaVisita"] = datetime.date.today().strftime('%d%m%y')


    # preparando la tabla de errores
    if not file_err_df.empty:
        logger.info("Preparando tabla 'errores'")
        file_err_df["nombreArchivo"] = filename
        errors_df = file_err_df[["nombreArchivo", "email", "tipoError"]].copy()

    return stats_df, visitors_df, errors_df


def load_statistics_table(stats_df: pd.DataFrame, conn: Connection) -> None:
    """ Esta función carga un datarame de estadisticas a su tabla correspondiente en sql """
    stats_df.to_sql(
        name="estadisticas",
        con=conn,
        if_exists='append',
        index=False
    )


def load_staging_visitors_table(visitors_df: pd.DataFrame, staging_table_name: str, conn: Connection) -> None:
    """ Esta función se encarga una dataframe temporal de visitantes a su tabla corrspondiente en sql """
    visitors_df.to_sql(
        name=staging_table_name,
        con =conn,
        if_exists='replace',
        index=False
    )
    # ejecutar código SQL para upsert en tabla visitantes real
    incremental_merge_query = text(f"""
        WITH agregados AS (
            SELECT * FROM {staging_table_name}
        )
        MERGE INTO visitantes AS T
        USING agregados AS S
        ON T.email = S.email
        WHEN MATCHED THEN
            UPDATE SET
                fechaPrimeraVisita = CASE
                                        WHEN S.fechaPrimeraVisita = T.fechaPrimeraVisita
                                        THEN S.fechaPrimeraVisita
                                        ELSE T.fechaPrimeraVisita
                                END,

                fechaUltimaVisita = CASE
                                    WHEN S.fechaUltimaVisita > T.fechaUltimaVisita
                                    THEN S.fechaUltimaVisita
                                    ELSE T.fechaUltimaVisita
                                END,

                visitasTotales = T.visitasTotales + S.visitasTotales

                visitasAnioActual = CASE
                                    WHEN EXTRACT(YEAR FROM T.fechaUltimaVisita) = EXTRACT(YEAR FROM CURRENT_DATE)
                                    THEN T.visitasAnioActual + S.visitasAnioActual
                                    ELSE S.visitasAnioActual
                                END,

                visitasMesActual = CASE
                                    WHEN EXTRACT(YEAR FROM T.fechaUltimaVisita) = EXTRACT(YEAR FROM CURRENT_DATE)
                                    AND EXTRACT(MONTH FROM T.fechaUltimaVisita) = EXTRACT(MONTH FROM S.fechaUltimaVisita)
                                    THEN T.visitasMesActual + S.visitasMesActual
                                    ELSE S.visitasMesActual
                                END,
        
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (email, fechaPrimeraVisita, fechaUltimaVisita, visitasTotales, visitasAnioActual, visitasMesActual)
            VALUES (S.email, S.fechaPrimeraVisita, S.fechaUltimaVisita, S.visitasTotales, S.visitasAnioActual, S.visitasMesActual);
    """
    )
    conn.execute(incremental_merge_query)   
    conn.execute(f"DROP TABLE  IF EXISTS {staging_table_name}")  # borramos tabla temporal


def load_errors_table(errors_df: pd.DataFrame, conn: Connection) -> None:
    """ Esta función carga un datarame de estadisticas a su tabla correspondiente en sql """
    errors_df.to_sql(
        name="errores",
        con=conn,
        if_exists='append',
        index=False
    )


def load_log_table(filename: str, stats_df: pd.DataFrame, errors_df: pd.DataFrame, conn: Connection) -> None:
    """ Esta función carga un registro nuevo en la tabla 'bitacora' """
    bitacora_dict = {
        "nombreArchivo": filename,
        "registrosExitosos": len(stats_df),
        "registrosFallidos": len(errors_df),
        "estatus": "Completado" if len(errors_df) > 0 else "Completado con errores"
    }
    pd.DataFrame(bitacora_dict).to_sql(
        name="bitacora",
        con=conn,
        if_exists='append',
    index=False 
    )


def move_to_backup(filepath: str) -> None:
    """ Esta función mueve un archivo descargado hasta el directorio del backup """
    # creamos directorio del backuo si no existe
    backup_dir = os.getenv("DIR_BACKUP")
    backup_dir.mkdir(parents=True, exists_ok=True)

    # creamos el path del archivo en el backup
    backup_path = backup_dir / filepath.name

    # movemos el archivo
    filepath.rename(backup_path)


def remove_from_sftp(filepath):
    """ Esta función remueve el archivo del directorio original en el servidor sftp """
    # definimos el directorio original
    sftp_dir = os.getenv("DIR_SFTP")

    # definimos el directorio del archivo original
    sftp_path = sftp_dir / filepath.name

    # removemos el archivo
    with sftp_connection() as sftp:
        sftp.remove(sftp_path)


def zip_compress() -> None:
    """Esta función encapsula varios archivos descargados y los comprime en un .zip """
    # definimos el directorio y la lista de archivos para el backup
    backup_dir = os.getenv("DIR_BACKUP")
    files_backup = [file for file in os.listdir(backup_dir) if file.startswith("report_") and file.endswith(".txt")]
    

    # creamos la ruta del .zip backup de hoy
    today_date = datetime.date.today().strftime('%d%m%y')
    zip_path = backup_dir / f"backup_{today_date}.zip" 

    # validamos que haya archivos para el backup
    if len(files_backup) > 0:
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for file in files_backup:
                zipf.write(file, file.name)
                file.unlink()

    







    
        

