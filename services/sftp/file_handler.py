"""SFTP file operations."""
import os
import stat
from config.settings import SFTP_REMOTE_DIR, TEMP_DOWNLOAD_DIR


class SFTPFileHandler:
    """Handles SFTP file operations."""
    
    def __init__(self, sftp_client):
        self.sftp = sftp_client
    
    def list_files(self):
        """List all files in the remote directory."""
        files = self.sftp.listdir_attr(SFTP_REMOTE_DIR)
        return files
    
    def download_file(self, remote_filename: str, local_path: str = None):
        """
        Download a file from SFTP server.
        
        :param remote_filename: Name of the file on the remote server
        :param local_path: Local path to save the file (defaults to TEMP_DOWNLOAD_DIR)
        :return: Path to the downloaded file
        """
        if local_path is None:
            if not TEMP_DOWNLOAD_DIR:
                raise ValueError("TEMP_DOWNLOAD_DIR is not set in environment variables")
            local_path = os.path.join(TEMP_DOWNLOAD_DIR, remote_filename)
        
        # Ensure the directory exists before downloading
        local_dir = os.path.dirname(local_path)
        if local_dir:
            os.makedirs(local_dir, exist_ok=True)
        
        remote_path = f"{SFTP_REMOTE_DIR}/{remote_filename}"
        self.sftp.get(remote_path, local_path)
        return local_path
    
    def is_regular_file(self, file_attr):
        """Check if file attribute represents a regular file."""
        return stat.S_ISREG(file_attr.st_mode)
    
    def is_hidden_file(self, filename: str):
        """Check if file is hidden (starts with dot)."""
        return filename.startswith(".")

