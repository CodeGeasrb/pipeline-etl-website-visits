from utils.utils import setup_logger, validate_file_loading, validate_file_layout, validate_data_quality, prepare_data


from typing import Tuple, Optional
from pathlib import Path
from prefect import task

import pandas as pd

        
@task(name="transformar datos", retries=2, retry_delay_seconds=60)
def transform(filepath: Path) -> Tuple[pd.DataFrame, Optional[pd.DataFrame]]:
    """ Valida la calidad de los datos para después hacer las transformaciones necesarias previo a la carga."""
    # inicializar el logger
    logger = setup_logger()
    logger.info("Iniciando etapa de transformación")

    # extraemos el nombre del archivo 
    filename = filepath.name

    # intentamos abrimos el archivo 
    logger.info(f"Abriendo el archivo {filename}...")
    file_df = validate_file_loading(filepath, logger)

    # comprobamos que el archivo no esté vacío
    if not file_df.empty():
        # validamos el layout del archivo
        logger.info("Validando el layout del archivo...")
        valid_layout = validate_file_layout(file_df, logger)
        
        # si el layout es válido, continuamos
        if valid_layout:
            # validamos la calidad de los datos
            logger.info("Validando la calidad de los datos...")
            file_ok_df, file_err_df = validate_data_quality(file_df, logger)

            # preparamos los datos para la carga
            logger.info("Preparando datos para la carga...")
            stats_df, errors_df = prepare_data(filename, file_ok_df, file_err_df, logger)
            return stats_df, errors_df
    logger.warning("Alerta: Este archivo no puede ser procesado")


























