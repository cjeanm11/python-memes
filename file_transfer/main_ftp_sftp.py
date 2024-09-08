import paramiko
import ftplib
import os
import logging
from typing import Optional
from dataclasses import dataclass
from abc import ABC, abstractmethod

class FileTransferClient(ABC):
    @abstractmethod
    def connect(self) -> None:
        """Establish the connection."""
        pass

    @abstractmethod
    def upload_file(self, local_file: str, remote_file: str) -> None:
        """Upload a file to the remote server."""
        pass

    @abstractmethod
    def close(self) -> None:
        """Close the connection."""
        pass

@dataclass
class ConnectionDetails(ABC):
    hostname: str
    port: int
    username: str
    password: Optional[str]
    local_file: str
    remote_file: str
    
@dataclass
class SFTPConnectionDetails(ConnectionDetails):
    hostname: str 
    port: int = 22
    username: str = ''
    password: Optional[str] = None
    ssh_key_file: Optional[str] = None  
    local_file: str = './data.parquet'
    remote_file: str = '/remote/path/data.parquet'

@dataclass
class FTPConnectionDetails(ConnectionDetails):
    hostname: str
    port: int = 21
    username: str = ''
    password: Optional[str] = None
    local_file: str = './data.parquet'
    remote_file: str = '/remote/path/data.parquet'


class FTPClient(FileTransferClient):
    def __init__(self, connection_details: FTPConnectionDetails):
        self.hostname = connection_details.hostname
        self.port = connection_details.port
        self.username = connection_details.username
        self.password = connection_details.password
        self.ftp: Optional[ftplib.FTP] = None

    def connect(self) -> None:
        """Establish the FTP connection."""
        try:
            self.ftp = ftplib.FTP()
            self.ftp.connect(self.hostname, self.port)
            self.ftp.login(self.username, self.password if self.password else '')
            logging.info(f"Connected to {self.hostname}")
        except ftplib.error_perm as e:
            logging.error(f"FTP permission error: {e}")
        except ftplib.all_errors as e:
            logging.error(f"FTP error occurred: {e}")
        except Exception as e:
            logging.error(f"An error occurred while connecting: {e}")

    def upload_file(self, local_file: str, remote_file: str) -> None:
        """Upload a file to the remote FTP server."""
        try:
            if self.ftp is None:
                raise RuntimeError("FTP client is not connected. Please call connect() before uploading files.")
            if not os.path.exists(local_file):
                raise FileNotFoundError(f"Local file {local_file} not found.")
            with open(local_file, 'rb') as file:
                self.ftp.storbinary(f'STOR {remote_file}', file)
                logging.info(f"File {local_file} uploaded to {remote_file}")
        except FileNotFoundError as e:
            logging.error(e)
        except RuntimeError as e:
            logging.error(e)
        except ftplib.error_perm as e:
            logging.error(f"FTP permission error: {e}")
        except Exception as e:
            logging.error(f"An error occurred during file upload: {e}")

    def close(self) -> None:
        """Close the FTP connection."""
        try:
            if self.ftp:
                self.ftp.quit()
                logging.info("FTP connection closed.")
        except Exception as e:
            logging.error(f"Error closing connection: {e}")

class SFTPClient(FileTransferClient):
    def __init__(self, sftp_connection_details: SFTPConnectionDetails):
        self.hostname = sftp_connection_details.hostname
        self.port = sftp_connection_details.port
        self.username = sftp_connection_details.username
        self.password = sftp_connection_details.password
        self.ssh_key_file = sftp_connection_details.ssh_key_file
        self.client: Optional[paramiko.SSHClient] = None
        self.sftp: Optional[paramiko.SFTPClient] = None

    def connect(self) -> None:
        """Establish the SFTP connection with either password or SSH key."""
        try:
            self.client = paramiko.SSHClient()
            self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

            if self.ssh_key_file:
                # Use SSH key authentication if provided
                private_key = paramiko.RSAKey.from_private_key_file(self.ssh_key_file)
                self.client.connect(self.hostname, port=self.port, username=self.username, pkey=private_key)
            else:
                # Use password authentication
                self.client.connect(self.hostname, port=self.port, username=self.username, password=self.password)

            self.sftp = self.client.open_sftp()
            logging.info(f"Connected to {self.hostname}")
        except paramiko.AuthenticationException:
            logging.error("Authentication failed. Please check your credentials or SSH key.")
        except paramiko.SSHException as e:
            logging.error(f"SSH error occurred: {e}")
        except Exception as e:
            logging.error(f"An error occurred while connecting: {e}")

    def upload_file(self, local_file: str, remote_file: str) -> None:
        """Upload a file to the remote SFTP server."""
        try:
            if self.sftp is None:
                raise RuntimeError("SFTP client is not connected. Please call connect() before uploading files.")
                
            if not os.path.exists(local_file):
                raise FileNotFoundError(f"Local file {local_file} not found.")
                
            self.sftp.put(local_file, remote_file)
            logging.info(f"File {local_file} uploaded to {remote_file}")
        except FileNotFoundError as e:
            logging.error(e)
        except RuntimeError as e:
            logging.error(e)
        except Exception as e:
            logging.error(f"An error occurred during file upload: {e}")

    def close(self) -> None:
        """Close the SFTP and SSH connection."""
        try:
            if self.sftp:
                self.sftp.close()
                logging.info("SFTP session closed.")
            if self.client:
                self.client.close()
                logging.info("SSH connection closed.")
        except Exception as e:
            logging.error(f"Error closing connection: {e}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    # Connection details for SFTP
    sftp_details: ConnectionDetails = SFTPConnectionDetails(
        hostname='example.com',
        username='username',
        password='password',
        local_file='./data.parquet',
        remote_file='/remote/path/data.parquet'
    )

    # SFTP client
    sftp_client: FileTransferClient = SFTPClient(sftp_details)
    sftp_client.connect()
    sftp_client.upload_file(sftp_details.local_file, sftp_details.remote_file)
    sftp_client.close()

    # Connection details for FTP
    ftp_details: ConnectionDetails = FTPConnectionDetails(
        hostname='example.com',
        username='username',
        password='password',
        local_file='./data.parquet',
        remote_file='/remote/path/data.parquet'
    )

    # FTP client
    ftp_client: FileTransferClient = FTPClient(ftp_details)
    ftp_client.connect()
    ftp_client.upload_file(ftp_details.local_file, ftp_details.remote_file)
    ftp_client.close()
    
