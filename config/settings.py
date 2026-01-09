"""Configuration settings loaded from environment variables."""
import os
from dotenv import load_dotenv

load_dotenv()

# SFTP Configuration
SFTP_HOST = os.getenv("SFTP_HOST")
SFTP_USER = os.getenv("SFTP_USER")
SFTP_KEY_PATH = os.getenv("SFTP_KEY_PATH")
SFTP_REMOTE_DIR = os.getenv("SFTP_REMOTE_DIR")
TEMP_DOWNLOAD_DIR = os.getenv("FILE_PATH")
START_DATE_STR = os.getenv("START_DATE")  # Format: DD-MM-YYYY

# Database Configuration
MONGO_URI = os.getenv("DB_URL")
DB_NAME = os.getenv("DB_NAME")

# Email Configuration
SMTP_SERVER = os.getenv("EMAIL_HOST")
SMTP_PORT = int(os.getenv("EMAIL_PORT", 587))
SMTP_USER = os.getenv("EMAIL_USER")
SMTP_PASS = os.getenv("EMAIL_PASSWORD")
FROM_EMAIL = os.getenv("EMAIL_FROM_USER")
TO_EMAIL = os.getenv("TO_EMAIL")

# Application Configuration
COMPANY_REF = os.getenv("COMPANY_REF")
FACILITY_REF = os.getenv("FACILITY_REF")
USER_INFO = os.getenv("USER_INFO")

# Validation
if not all([SFTP_HOST, SFTP_USER, SFTP_KEY_PATH, SFTP_REMOTE_DIR, MONGO_URI]):
    raise RuntimeError("Missing one or more required environment variables in .env")

if not all([SMTP_SERVER, SMTP_USER, SMTP_PASS, FROM_EMAIL, TO_EMAIL]) or SMTP_PORT not in [25, 465, 587]:
    raise RuntimeError(
        "Missing/invalid email environment variables in .env (supported ports: 25, 465, 587). "
        "Also, ensure TO_EMAIL is set."
    )

# Ensure local folder exists
if TEMP_DOWNLOAD_DIR:
    os.makedirs(TEMP_DOWNLOAD_DIR, exist_ok=True)

