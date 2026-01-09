"""Centralized logging configuration with daily log file rotation."""
import logging
import os
from datetime import datetime
from logging import Handler


class DailyRotatingFileHandler(Handler):
    """Custom handler that creates a new log file each day with date in filename."""
    
    def __init__(self, log_dir="logs"):
        super().__init__()
        self.log_dir = log_dir
        self.current_date = None
        self.current_file = None
        self.current_handler = None
        
        # Create logs directory if it doesn't exist
        os.makedirs(log_dir, exist_ok=True)
        
        # Initialize with today's date
        self._update_file_if_needed()
    
    def _update_file_if_needed(self):
        """Update log file if date has changed."""
        today = datetime.now().strftime("%Y-%m-%d")
        
        if self.current_date != today:
            # Close old handler if exists
            if self.current_handler:
                self.current_handler.close()
            
            # Create new log file with only date in filename
            log_file = os.path.join(self.log_dir, f"{today}.log")
            self.current_file = log_file
            self.current_date = today
            
            # Create new file handler
            self.current_handler = logging.FileHandler(
                log_file,
                encoding='utf-8'
            )
            
            # Set formatter
            formatter = logging.Formatter(
                '%(asctime)s - %(levelname)s - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            self.current_handler.setFormatter(formatter)
    
    def emit(self, record):
        """Emit a record, creating new file if date changed."""
        self._update_file_if_needed()
        if self.current_handler:
            self.current_handler.emit(record)
    
    def close(self):
        """Close the handler."""
        if self.current_handler:
            self.current_handler.close()
        super().close()


def setup_logger(log_dir="logs"):
    """
    Set up a logger that creates daily log files with date in filename.
    Log files are named: YYYY-MM-DD.log
    
    Args:
        log_dir: Directory where log files will be stored
        
    Returns:
        Configured logger instance
    """
    # Create logger
    logger = logging.getLogger("knowella_bg_jobs")
    logger.setLevel(logging.INFO)
    
    # Remove existing handlers to avoid duplicates
    logger.handlers = []
    
    # Create custom daily rotating handler
    file_handler = DailyRotatingFileHandler(log_dir)
    
    # Add handler to logger
    logger.addHandler(file_handler)
    
    return logger


# Initialize the logger
logger = setup_logger()

