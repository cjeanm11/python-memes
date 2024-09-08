import unittest, logging
from unittest.mock import patch, MagicMock
from main_ftp_sftp import FTPConnectionDetails, FTPClient, FileTransferClient, SFTPClient, SFTPConnectionDetails

class TestFileTransferClients(unittest.TestCase):

    @patch('ftplib.FTP')
    def test_ftp_client(self, MockFTP):
        mock_ftp = MockFTP.return_value
        mock_ftp.storbinary.return_value = None
        
        ftp_details = FTPConnectionDetails(
            hostname='test_ftp_server',
            username='test_user',
            password='test_pass',
            local_file='./test_file.txt',
            remote_file='/remote/path/test_file.txt'
        )
        
        # Mocking os.path.exists
        with patch('os.path.exists') as mock_exists:
            mock_exists.return_value = True
            
            ftp_client = FTPClient(ftp_details)
            ftp_client.connect()
            ftp_client.upload_file(ftp_details.local_file, ftp_details.remote_file)
            ftp_client.close()
            
            mock_ftp.connect.assert_called_once_with('test_ftp_server', 21)
            mock_ftp.login.assert_called_once_with('test_user', 'test_pass')
            mock_ftp.storbinary.assert_called_once_with('STOR /remote/path/test_file.txt', unittest.mock.ANY)
            mock_ftp.quit.assert_called_once()

    @patch('paramiko.SSHClient')
    def test_sftp_client(self, MockSSHClient):
        mock_ssh_client = MockSSHClient.return_value
        mock_sftp = mock_ssh_client.open_sftp.return_value
        mock_sftp.put.return_value = None
        
        sftp_details = SFTPConnectionDetails(
            hostname='test_sftp_server',
            username='test_user',
            password='test_pass',
            local_file='./test_file.txt',
            remote_file='/remote/path/test_file.txt'
        )
        
        # Mocking os.path.exists
        with patch('os.path.exists') as mock_exists:
            mock_exists.return_value = True
            sftp_client = SFTPClient(sftp_details)
            sftp_client.connect()
            sftp_client.upload_file(sftp_details.local_file, sftp_details.remote_file)
            sftp_client.close()
            
            mock_ssh_client.set_missing_host_key_policy.assert_called_once()

# Setup logging
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    unittest.main()