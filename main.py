import schedule
import time
import os
from threading import Lock
from services.processing.file_processor import fetch_files_from_sftp
from dotenv import load_dotenv
from process_csv import *
from helpers import apis, dateTime_helper  
from helpers.logger import logger

load_dotenv()

job_lock = Lock()

def job():
    if job_lock.locked():
        return
    with job_lock:
        try:
            fetch_files_from_sftp()
        except Exception as e:
            logger.error(f"Error during fetch: {e}")

# Read time interval from environment variable (in minutes)
TIME_IN_MINUTES = int(os.getenv("TIME_INTERVEL", 1))  # default 1 minute

schedule.every(TIME_IN_MINUTES).minutes.do(job)

fetch_files_from_sftp()

# for testing
while True:
    schedule.run_pending()
    time.sleep(1)  # check every second

