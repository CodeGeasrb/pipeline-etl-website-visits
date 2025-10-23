from utils.utils import setup_logger, validate_file_loading


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
def transform(filepath: Path) -> Tuple (pd.DataFrame, pd.DataFrame, pd.DataFrame):
    """ Valida la calidad de los datos para después hacer las transformaciones necesarias previo a la carga
        Todo mediante el uso de dataframes de pandas """
    # inicializar el logger
    logger = setup_logger()

    # logging inicial
    logger.info("Iniciando etapa de transformación")

    # logging de abrir el archivo
    logger.info(f"Abriendo el archivo {filepath.split("/")[-1]}...")

    # abrimos el archivo
    file = validate_file_loading(filepath, logger)

    # validamos apertura del archivo y comenzamos a transformar
    if file is not None:

























