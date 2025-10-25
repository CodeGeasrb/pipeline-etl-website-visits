from sqlalchemy import text
from sqlalchemy import Connection


import pandas as pd
import os


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



