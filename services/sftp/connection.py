"""SFTP connection management."""
import paramiko
from config.settings import SFTP_HOST, SFTP_USER, SFTP_KEY_PATH
from services.sftp.key_loader import load_private_key
from helpers.logger import logger


class SFTPConnection:
    """Manages SFTP connection lifecycle."""
    
    def __init__(self):
        self.ssh = None
        self.sftp = None
        self.key = None
    
    def connect(self):
        """Establish SFTP connection."""
        self.key = load_private_key(SFTP_KEY_PATH)
        self.ssh = paramiko.SSHClient()
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        
        # Try connection with better error handling
        try:
            self.ssh.connect(
                hostname=SFTP_HOST,
                username=SFTP_USER,
                pkey=self.key,
                timeout=30,
                allow_agent=False,
                look_for_keys=False
            )
        except paramiko.AuthenticationException as auth_err:
            # If that fails and key_path is a .pem file, try using key_filename with explicit key type
            if SFTP_KEY_PATH.endswith('.pem') or SFTP_KEY_PATH.endswith('.key'):
                try:
                    # Reload the key to ensure we have a fresh instance
                    key_reload = load_private_key(SFTP_KEY_PATH)
                    ssh2 = paramiko.SSHClient()
                    ssh2.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                    # Use pkey instead of key_filename to avoid auto-detection issues
                    ssh2.connect(
                        hostname=SFTP_HOST,
                        username=SFTP_USER,
                        pkey=key_reload,
                        timeout=30,
                        allow_agent=False,
                        look_for_keys=False
                    )
                    self.ssh = ssh2  # Use the working connection
                except Exception as e2:
                    logger.error(f"SFTP connection failed: {e2}")
                    raise
            else:
                logger.error(f"SFTP authentication failed: {auth_err}")
                raise
        except Exception as e:
            logger.error(f"SFTP connection error: {e}")
            raise
        
        logger.info("Connection between SFTP server")
        self.sftp = self.ssh.open_sftp()
        return self.sftp
    
    def close(self):
        """Close SFTP and SSH connections."""
        if self.sftp:
            self.sftp.close()
        if self.ssh:
            self.ssh.close()
    
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
        return False

