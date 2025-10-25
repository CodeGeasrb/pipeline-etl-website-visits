from prefect.task_runners import ConcurrentTaskRunner
from prefect import flow

from flows.etl_flow import etl_flow

from tasks.post_processing import compress_backup
from tasks.pre_processing import list_files

import logging


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
        return []   # no velovemos nada

    # logging de cuantos archivos a procesar existen
    logger.info(f"Se procesarán {len(files)} archivos")

    # 2. Procesamiento (ETL) paralelo de archivos
    logger.info("Añadiendo archivos a cola de procesamiento...")
    subflows_results = []   
    for filepath in files:
        run = etl_flow.submit(filepath) # ejecutamos cada el proceso ETL para cada archivo de forma paralela
        subflows_results.append(run) # guardamos los resultados de las ejecuciones

    # esperamos a que termine de procesarse cada archivo
    logger.info("Proceso ETL ejecutandose...")
    results = [r.result() for r in subflows_results]

    # 3. Creación de backup
    logger.info("Creamos el backup de hoy")
    compress_backup()

    # logging de finalización
    logger.info("=" * 80)
    logger.info("Proceso ETL de Visitas Web completado")
    logger.info("=" * 80)

    return results



