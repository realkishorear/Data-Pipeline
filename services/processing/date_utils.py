"""Date parsing utilities."""
from datetime import datetime
from config.settings import START_DATE_STR


def parse_start_date(start_date_str: str = None):
    """
    Parse START_DATE from DD-MM-YYYY format to datetime object.
    Returns datetime set to start of day (00:00:00) or None if not set or invalid.
    This ensures files from the start date onwards are processed.
    """
    if not start_date_str:
        start_date_str = START_DATE_STR
    
    if not start_date_str:
        return None
    
    try:
        # Parse date and set to start of day (00:00:00) to include all files from that day
        parsed_date = datetime.strptime(start_date_str.strip(), "%d-%m-%Y")
        # Ensure time is set to 00:00:00 (start of day)
        return parsed_date.replace(hour=0, minute=0, second=0, microsecond=0)
    except ValueError:
        return None


def extract_date_from_filename(filename: str):
    """
    Extract date from filename format: PREFIXYYYYMMDDHHMMSS.csv
    Returns datetime object or None if extraction fails.
    """
    try:
        # Remove .csv extension if present
        name_without_ext = filename.replace(".csv", "")
        # Filename format: PREFIX(2 chars) + YYYYMMDD(8 chars) + HHMMSS(6 chars)
        if len(name_without_ext) >= 10:  # At least 2 (prefix) + 8 (date)
            date_str = name_without_ext[2:10]  # Extract YYYYMMDD
            return datetime.strptime(date_str, "%Y%m%d")
    except (ValueError, IndexError):
        pass
    return None

