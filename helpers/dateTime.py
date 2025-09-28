import math
import re
import logging
from datetime import datetime

# ✅ Configure logging
logging.basicConfig(
    level=logging.INFO,   # Use logging.DEBUG for more granular tracing
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def excel_date_to_js_date(serial: float) -> datetime:
    """
    Convert an Excel date serial number to a Python datetime object.
    Excel serial 25569 = 1970-01-01.
    """
    logging.info(f"[excel_date_to_js_date] Received serial: {serial}")

    try:
        # Days since Unix epoch
        utc_days = math.floor(serial - 25569)
        utc_value = utc_days * 86400  # seconds since epoch
        date_info = datetime.utcfromtimestamp(utc_value)
        logging.debug(f"[excel_date_to_js_date] Base date (UTC): {date_info}")

        # Fractional part for time
        fractional_day = serial - math.floor(serial) + 0.0000001  # avoid floating point errors
        total_seconds = math.floor(86400 * fractional_day)
        seconds = total_seconds % 60
        total_seconds -= seconds
        hours = math.floor(total_seconds / 3600)
        minutes = math.floor(total_seconds / 60) % 60

        logging.debug(
            f"[excel_date_to_js_date] Time extracted => Hours: {hours}, "
            f"Minutes: {minutes}, Seconds: {seconds}"
        )

        final_date = datetime(
            date_info.year,
            date_info.month,
            date_info.day,
            hours,
            minutes,
            seconds
        )
        logging.info(f"[excel_date_to_js_date] Final datetime: {final_date}")
        return final_date

    except Exception as e:
        logging.exception(f"[excel_date_to_js_date] Error converting serial: {e}")
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
    logging.info(f"[check_date_and_time] Processing value: {date_value} "
                 f"(type: {type(date_value).__name__})")

    # Case 1: Excel serial (number)
    if isinstance(date_value, (int, float)):
        logging.debug("[check_date_and_time] Detected Excel serial date")
        try:
            date_obj = excel_date_to_js_date(date_value)
            result = {
                "isValidDate": isinstance(date_obj, datetime),
                "hasTime": False,
                "date": date_obj
            }
            logging.info(f"[check_date_and_time] Excel serial result: {result}")
            return result
        except Exception as e:
            logging.exception(f"[check_date_and_time] Failed to convert Excel serial: {e}")
            return {"isValidDate": False, "hasTime": False, "date": None}

    # Case 2: String date
    elif isinstance(date_value, str):
        logging.debug("[check_date_and_time] Detected string date")
        parts = date_value.strip().split(" ")
        date_parts = parts[0].split("/")

        logging.debug(f"[check_date_and_time] Split parts => date_parts: {date_parts}, "
                      f"time_parts: {parts[1:] if len(parts) > 1 else None}")

        if len(date_parts) != 3:
            logging.warning("[check_date_and_time] Invalid date format (expected YYYY/MM/DD)")
            return {"isValidDate": False, "hasTime": False, "date": None}

        try:
            year = int(date_parts[0])
            month = int(date_parts[1]) - 1  # JS months are 0-based
            day = int(date_parts[2])
            logging.debug(
                f"[check_date_and_time] Parsed values => Year: {year}, Month(JS): {month}, Day: {day}"
            )

            # Convert to Python datetime (month back to 1-based)
            py_month = month + 1
            date_obj = datetime(year, py_month, day, 0, 0, 0, 0)

            # Check if a time string exists and matches HH:MM
            has_time = False
            if len(parts) > 1 and re.match(r"^(\d{2}):(\d{2})$", parts[1]):
                has_time = True
                logging.debug("[check_date_and_time] Valid time component detected")

            # Validate date
            is_valid = (
                date_obj.year == year and
                date_obj.month == py_month and
                date_obj.day == day
            )
            logging.info(
                f"[check_date_and_time] String date result => isValid: {is_valid}, hasTime: {has_time}"
            )

            return {
                "isValidDate": is_valid,
                "hasTime": has_time,
                "date": date_obj
            }

        except (ValueError, TypeError) as e:
            logging.exception(f"[check_date_and_time] Parsing error: {e}")
            return {"isValidDate": False, "hasTime": False, "date": None}

    # Unsupported type
    logging.warning(f"[check_date_and_time] Unsupported type: {type(date_value).__name__}")
    return {"isValidDate": False, "hasTime": False, "date": None}
