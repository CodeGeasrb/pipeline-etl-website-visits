from contextlib import contextmanager
import paramiko
import os



@contextmanager
def sftp_connection():
    """ Set  up de la conexión con el servidor de inicio mediante SFTP """
    # inicializamos los recursos de conexión como None
    transport = None
    sftp = None

    # intentar conexión a servidor sftp
    try:
        # se definen las credenciales de conexión
        port = int(os.getenv("PORT_SFTP"))
        host = os.getenv("HOST_SFTP")
        # establece la conexion mediante sftp
        transport = paramiko.Transport((host, port))
        transport.connect(username=os.getenv("USER_SFTP"), password=os.getenv("PASSWORD_SFTP"))
        sftp = paramiko.SFTPClient.from_transport(transport)
        # cedemos control al with
        yield sftp

    finally:
        # cerramos la conexión si está fue establecida
        if sftp is not None:
            sftp.close()
        if transport is not None:
            transport.close()
            

