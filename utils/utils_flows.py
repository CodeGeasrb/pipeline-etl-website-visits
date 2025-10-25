import datetime
import logging
import os


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