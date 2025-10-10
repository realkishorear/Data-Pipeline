import schedule
import time
import os
from threading import Lock
from sftp import fetch_files_from_sftp
from dotenv import load_dotenv
from process_csv import *
from helpers import apis, dateTime_helper  

load_dotenv()

job_lock = Lock()

def job():
    if job_lock.locked():
        print("Previous job still running. Skipping this run.")
        return
    with job_lock:
        print("Job started.")
        try:
            fetch_files_from_sftp()
        except Exception as e:
            print(f"Error during fetch: {e}")
        print("Job finished.")

# Read time interval from environment variable (in minutes)
TIME_IN_MINUTES = int(os.getenv("TIME_INTERVEL", 1))  # default 1 minute
print(f"Scheduler started. Running every {TIME_IN_MINUTES} minutes.")

schedule.every(TIME_IN_MINUTES).minutes.do(job)

def test():
    fetch_files_from_sftp()
test()

while True:
    schedule.run_pending()
    time.sleep(1)  # check every second

