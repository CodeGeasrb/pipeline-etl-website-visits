from utils.utils import list_files, setup_logger
from tasks.extract import extract
from tasks.transform import transform
from tasks.load import load
from tasks.post_processing import post_processing, compress_backup

from prefect.task_runners import ConcurrentTaskRunner
from prefect import flow

import logging


@flow(
    name="Proceso ETL (micro-batch)",
    task_runner = ConcurrentTaskRunner(max_workers=2),
    retries=1,
    retry_delay_seconds=60
)
def etl_flow(filepath: str): 
    """ Esta tarea que define el flujo de procesamiento del ETL por archivo """
    # Obtener el nombre del file
    filename = filepath.name

    # inicializar archivo de logging
    logger = setup_logger(filename)
    logger.info(f"=== Iniciando proceso ETL para archivo: {filename} ===")

    # intentamos correr el flujo ETL
    try:
        filepath = extract(filename, logger)    # extract
        stats_df, visitors_df, errors_df = transform(filepath, logger)   # transform
        load(filename, visitors_df, stats_df, errors_df, logger) # load
        post_processing(filepath, logger)
        logger.info(f"=== Archivo {filename} procesado con éxito ===")
    except Exception as e:
        logger.error(f"Error: Fallo procesando el archivo: {filename}")
        raise e


@flow(
    name="Orquestación de ETL de visitas sitio web",
    task_runner = ConcurrentTaskRunner(max_workers=2)
)
def etl_flow_orchestration():
    """ Esta es la tarea que define el flujo de orquestación del ETL para procesar todos los archivos nuevos """
    # Inicializamos el logger y loggings iniciales
    logger = logging.getLogger("orchestrator")
    logger.info("=" * 80)
    logger.info("Iniciando ETL de Visitas Web")
    logger.info("=" * 80)

    # 1. Listar archivos nuevos
    logger.info("Enlistamos archivos nuevos...")
    files = list_files()

    # valimos que se hayan encontrado archivos
    if not files: 
        logger.warning("Alerta: No hay archivos nuevos para procesar")

    # logging de cuantos archivos a procesar existen
    logger.info(f"Se procesarán {len(files)} archivos")

    # 2. Procesamiento (ETL) paralelo de archivos
    logger.info("Añadiendo archivos a cola de procesamiento...")
    subflows_results = []   
    for filepath in files:
        run = etl_flow().submit(filepath) # ejecutamos cada el proceso ETL para cada archivo de forma paralela
        subflows_results.append(run) # guardamos los resultados de las ejecuciones

    # esperamos a que termine de procesarse cada archivo
    logger.info("Proceso ETL ejecutandose...")
    results = [r.results() for r in subflows_results]

    # 3. Creación de backup
    logger.info("Creamos el backup de hoy")
    compress_backup()

    # logging de finalización
    logger.info("=" * 80)
    logger.info("Proceso ETL de Visitas Web completado")
    logger.info("=" * 80)



