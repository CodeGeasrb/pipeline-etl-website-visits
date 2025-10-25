from sqlalchemy.pool import QueuePool
from utils.utils import create_mysql_connection_url, load_statistics_table,load_staging_visitors_table, load_errors_table, load_log_table
from prefect import task
from sqlalchemy import create_engine, text

import pandas as pd
import logging




@task(name="cargar datos", retries=2, retry_delay_seconds=60)
def load(filename: str, stats_df: pd.DataFrame, visitors_df: pd.DataFrame, errors_df: pd.DataFrame, logger: logging.Logger) -> None:
    """ Esta tarea carga los datos del archivo contenidos a las tablas estadísticas y errores de una base de datos mysql 
    y después hace el update de la tabla visitantes """
    # iniciamos el logging de la tarea
    logger.info("Iniciando etapa de carga")

    # establecemos conexión con el servidor mysql
    mysql_conn_url = create_mysql_connection_url()
    mysql_engine = create_engine(
        mysql_conn_url,
        poolclass=QueuePool,
        pool_size=100    # 100 conexiones simultaneas permitidas
        )

    # cargamos las tablas 
    with mysql_engine.begin() as conn:
        # intentamos cargar las tablas
        try:
            # cargamos tabla de estadísticas
            logger.info("Insertando tabla 'estadisticas'")
            load_statistics_table(stats_df, conn)
            
            # cargamos tabla visitantes temporal (staging)
            logger.info("Actualizando tabla 'visitantes'")
            staging_table_name = f"visitantes_{filename}"
            load_staging_visitors_table(visitors_df, staging_table_name, conn)
            
            # cargamos tabla 'errores'
            if len(errors_df) > 0:
                logger.info("Insertando tabla 'errores'")
                load_errors_table(errors_df, conn)

            # creamos registro de bitacora
            load_log_table(filename, stats_df, errors_df, conn)
        
        except Exception as e:
            logger.error(f"Error: No se pudo cargar la información a la base de datos. {str(e)}")
            raise e
            

            


















