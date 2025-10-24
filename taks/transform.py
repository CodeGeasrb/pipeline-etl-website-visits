from utils.utils import setup_logger, validate_file_loading, validate_file_layout, validate_data_quality


from typing import Tuple
from pathlib import Path
from prefect import task

import logging
import pandas as pd


# Columnas esperadas por archivo
VALID_COLUMNS = [
        'email', 'fecha_visita', 'jyv', 'Badmail', 'Baja', 
        'Fecha_envio', 'Fecha_open', 'Opens', 'Opens_virales',
        'Fecha_click', 'Clicks', 'Clicks_virales', 'Links', 
        'IPs', 'Navegadores', 'Plataformas'
]
            
        
@task(name="transformar datos", retries=2, retry_delay_seconds=60)
def transform(filepath: Path) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """ 
    Valida la calidad de los datos para después hacer las transformaciones 
    necesarias previo a la carga.
    """
    # inicializar el logger
    logger = setup_logger()
    logger.info("Iniciando etapa de transformación")

    # extraemos el nombre del archivo 
    filename = filepath.name
    logger.info(f"Abriendo el archivo {filename}...")

    # abrimos el archivo
    file_df = validate_file_loading(filepath, logger)

    # validamos apertura del archivo
    if file_df is not None:
        
        # validamos el layout del archivo
        logger.info("Validando el layout del archivo...")
        valid_layout = validate_file_layout(file_df, VALID_COLUMNS, logger)
        if valid_layout:
            # validamos la calidad de los datos y separamos entre registros validos y no válidos
            logger.info("Validando la calidad de los datos...")
            file_ok_df, file_err_df = validate_data_quality(file_df, logger)

            # transformamos los datos para prepararlos para la carga
            logger.info("Transformando datos...")




























