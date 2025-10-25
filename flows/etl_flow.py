from importlib.abc import FileLoader
from prefect.task_runners import ConcurrentTaskRunner
from prefect import flow
from pathlib import Path

from utils.utils import setup_logger

from tasks.extract import extract
from tasks.transform import transform
from tasks.load import load
from tasks.post_processing import post_processing


@flow(
    name="Proceso ETL (micro-batch)",
    task_runner = ConcurrentTaskRunner(max_workers=2),
    retries=1,
    retry_delay_seconds=60
)
def etl_flow(filepath: str): 
    """ Este es el flujo que define el procesamiento del ETL por archivo """
    # Obtener el nombre del file
    filename = filepath.name

    # pasar filepath string a Path
    if isinstance(filepath, str):
        filepath = Path(FileLoader)

    # inicializar archivo de logging
    logger = setup_logger(filename)
    logger.info(f"=== Iniciando proceso ETL para archivo: {filename} ===")

    # intentamos correr el flujo ETL
    try:
        filepath = extract(filename, logger)    # extract

        stats_df, visitors_df, errors_df = transform(filepath, logger)   # transform

        load(filename, visitors_df, stats_df, errors_df, logger) # load

        post_processing(filepath, logger)   # post processing
        logger.info(f"=== Archivo {filename} procesado con éxito ===")
        return True

    except Exception as e:
        logger.error(f"Error: Fallo procesando el archivo: {filename}")
        raise e




