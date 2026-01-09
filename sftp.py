"""
SFTP module - Legacy wrapper for backward compatibility.
This module now delegates to the refactored services.
"""
from services.processing.file_processor import fetch_files_from_sftp, check_sftp as checkSFTP

# Export for backward compatibility
__all__ = ['fetch_files_from_sftp', 'checkSFTP']
