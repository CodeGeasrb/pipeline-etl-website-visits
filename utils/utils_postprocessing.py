from utils.utils_extract import sftp_connection

import datetime
import zipfile
import os


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



                
