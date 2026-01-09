import math
import re
from datetime import datetime
from helpers.logger import logger

def excel_date_to_js_date(serial: float) -> datetime:
    """
    Convert an Excel date serial number to a Python datetime object.
    Excel serial 25569 = 1970-01-01.
    """
    try:
        # Days since Unix epoch
        utc_days = math.floor(serial - 25569)
        utc_value = utc_days * 86400  # seconds since epoch
        date_info = datetime.utcfromtimestamp(utc_value)

        # Fractional part for time
        fractional_day = serial - math.floor(serial) + 0.0000001  # avoid floating point errors
        total_seconds = math.floor(86400 * fractional_day)
        seconds = total_seconds % 60
        total_seconds -= seconds
        hours = math.floor(total_seconds / 3600)
        minutes = math.floor(total_seconds / 60) % 60

        final_date = datetime(
            date_info.year,
            date_info.month,
            date_info.day,
            hours,
            minutes,
            seconds
        )
        return final_date

    except Exception as e:
        logger.error(f"[excel_date_to_js_date] Error converting serial: {e}")
        raise


def check_date_and_time(date_value):
    """
    Check if the provided value is a valid date/time in either
    Excel serial format or a string (YYYY/MM/DD [HH:MM]).
    Returns a dict with:
        isValidDate -> bool
        hasTime     -> bool
        date        -> datetime | None
    """
    # Case 1: Excel serial (number)
    if isinstance(date_value, (int, float)):
        try:
            date_obj = excel_date_to_js_date(date_value)
            result = {
                "isValidDate": isinstance(date_obj, datetime),
                "hasTime": False,
                "date": date_obj
            }
            return result
        except Exception as e:
            logger.error(f"[check_date_and_time] Failed to convert Excel serial: {e}")
            return {"isValidDate": False, "hasTime": False, "date": None}

    # Case 2: String date
    elif isinstance(date_value, str):
        parts = date_value.strip().split(" ")
        date_parts = parts[0].split("/")

        if len(date_parts) != 3:
            return {"isValidDate": False, "hasTime": False, "date": None}

        try:
            year = int(date_parts[0])
            month = int(date_parts[1]) - 1  # JS months are 0-based
            day = int(date_parts[2])

            # Convert to Python datetime (month back to 1-based)
            py_month = month + 1
            date_obj = datetime(year, py_month, day, 0, 0, 0, 0)

            # Check if a time string exists and matches HH:MM
            has_time = False
            if len(parts) > 1 and re.match(r"^(\d{2}):(\d{2})$", parts[1]):
                has_time = True

            # Validate date
            is_valid = (
                date_obj.year == year and
                date_obj.month == py_month and
                date_obj.day == day
            )

            return {
                "isValidDate": is_valid,
                "hasTime": has_time,
                "date": date_obj
            }

        except (ValueError, TypeError) as e:
            logger.error(f"[check_date_and_time] Parsing error: {e}")
            return {"isValidDate": False, "hasTime": False, "date": None}

    # Unsupported type
    return {"isValidDate": False, "hasTime": False, "date": None}
