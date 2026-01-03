import os
import stat
from datetime import datetime
import paramiko
from bson import ObjectId
from pymongo import MongoClient
from dotenv import load_dotenv
from process_csv import process_csv_file
import smtplib
from email.mime.text import MIMEText
import ssl  # For SSL context

# ── Load environment variables ────────────────────────────────────────────────
load_dotenv()

SFTP_HOST = os.getenv("SFTP_HOST")
SFTP_USER = os.getenv("SFTP_USER")
SFTP_KEY_PATH = os.getenv("SFTP_KEY_PATH")
SFTP_REMOTE_DIR = os.getenv("SFTP_REMOTE_DIR")
MONGO_URI = os.getenv("DB_URL")
TEMP_DOWNLOAD_DIR = os.getenv("FILE_PATH")  # temporary download folder

# Email configuration from .env (matched to your provided variable names)
SMTP_SERVER = os.getenv("EMAIL_HOST")
SMTP_PORT = int(os.getenv("EMAIL_PORT", 587))
SMTP_USER = os.getenv("EMAIL_USER")
SMTP_PASS = os.getenv("EMAIL_PASSWORD")
FROM_EMAIL = os.getenv("EMAIL_FROM_USER")
TO_EMAIL = os.getenv("TO_EMAIL")  # Ensure this is set in your .env file (e.g., TO_EMAIL=your.email@example.com)

if not all([SFTP_HOST, SFTP_USER, SFTP_KEY_PATH, SFTP_REMOTE_DIR, MONGO_URI]):
    raise RuntimeError(
        "❌ Missing one or more required environment variables in .env")

# Check email env vars (updated to match your names)
if not all([SMTP_SERVER, SMTP_USER, SMTP_PASS, FROM_EMAIL, TO_EMAIL]) or SMTP_PORT not in [25, 465, 587]:
    raise RuntimeError(
        "❌ Missing/invalid email environment variables in .env (supported ports: 25, 465, 587). Also, ensure TO_EMAIL is set.")

# ── Ensure local folder exists ────────────────────────────────────────────────
os.makedirs(TEMP_DOWNLOAD_DIR, exist_ok=True)

# ── MongoDB setup ─────────────────────────────────────────────────────────────
client = MongoClient(MONGO_URI)
db = client.get_default_database()
print(f"[✅] Connected to MongoDB database: {db.name}")

checklist_map_col = db["checklistmaps"]
inspections_col = db["inspections"]
file_uploads_col = db["checklistfileuploads"]

# ── Helper functions ──────────────────────────────────────────────────────────

def ensure_prefix_in_db(prefix: str):
    mapping = checklist_map_col.find_one({
        "acronym": {"$regex": f"^{prefix.strip()}$", "$options": "i"}
    })
    if mapping:
        print(f"[DEBUG] Found mapping for prefix '{prefix}': {mapping}")
        return mapping.get("checklistRef")
    else:
        print(f"[DEBUG] No mapping found for prefix '{prefix}'")
        return None

def create_system_inspection(company_ref: str, facility_ref: str, checklist_ref: str) -> str:
    inspection_payload = {
        "status": "Pending",
        "isActive": True,
        "isUnSchedule": True,
        "isDeleted" : False,
        "companyRef": ObjectId(company_ref),
        "facilityRef": ObjectId(facility_ref),
        "checklistRef": ObjectId(checklist_ref),
        "assignee": [],
        "followers": [],
        "createdAt": datetime.utcnow(),
        "updatedAt": datetime.utcnow(),
        "__v" : 0
    }
    result = inspections_col.insert_one(inspection_payload)
    print(f"[INFO] ✅ New inspection created with ID: {result.inserted_id}")
    return str(result.inserted_id)

def send_email(subject: str, body: str):
    """Send an email notification."""
    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = FROM_EMAIL
    msg['To'] = TO_EMAIL

    context = ssl.create_default_context()  # For better SSL handling

    try:
        if SMTP_PORT == 465:
            # Use SMTP_SSL for port 465 (SSL)
            with smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT, context=context) as server:
                server.login(SMTP_USER, SMTP_PASS)
                server.sendmail(FROM_EMAIL, TO_EMAIL, msg.as_string())
        else:
            # Use SMTP with STARTTLS for port 587 or 25
            with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
                server.starttls(context=context)
                server.login(SMTP_USER, SMTP_PASS)
                server.sendmail(FROM_EMAIL, TO_EMAIL, msg.as_string())
        print(f"[✅] Email sent: {subject}")
    except smtplib.SMTPAuthenticationError as e:
        print(f"[❌] Authentication failed: {e} - Ensure your SendGrid API key is correct and 'apikey' is used as username.")
    except smtplib.SMTPServerDisconnected as e:
        print(f"[❌] Server disconnected: {e} - Check port (465 for SSL) and network/firewall.")
    except smtplib.SMTPRecipientsRefused as e:
        print(f"[❌] Recipient refused: {e} - Verify the 'From' email is authenticated in SendGrid and 'To' email is valid.")
    except Exception as e:
        print(f"[❌] Error sending email: {type(e).__name__}: {e}")

# ── Main SFTP processing function ─────────────────────────────────────────────

def checkSFTP():
    print("=" * 60)
    print(
        f"[Checking SFTP connection] : checkSFTP()")
    print("=" * 60)
    
    key = paramiko.RSAKey.from_private_key_file(SFTP_KEY_PATH)
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    try:
        print(f"[INFO] Connecting to SFTP: {SFTP_HOST}")
        ssh.connect(hostname=SFTP_HOST, username=SFTP_USER, pkey=key)
        print("[✅] SFTP connection established.")
        sftp = ssh.open_sftp()

        files = sftp.listdir_attr(SFTP_REMOTE_DIR)
        print(files)
        
        print(f"[INFO] Total files found: {len(files)}")

        sftp.close()
        ssh.close()
        print("[✅] SFTP connection closed.")

    except Exception as e:
        print(f"[❌] SFTP Process Error: {e}")

def fetch_files_from_sftp():
    print("=" * 60)
    print(
        f"[fetch_files_from_sftp] Cron triggered at: {datetime.utcnow().isoformat()}")
    print("=" * 60)

    # Step 01 : SFTP Connection Establishment
    key = paramiko.RSAKey.from_private_key_file(SFTP_KEY_PATH)
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    processed = successful = failed = 0
    company_ref = os.getenv("COMPANY_REF")
    facility_ref = os.getenv("FACILITY_REF")
    userinfo = os.getenv("USER_INFO")
    
    try:
        print(f"[INFO] Connecting to SFTP: {SFTP_HOST}")
        ssh.connect(hostname=SFTP_HOST, username=SFTP_USER, pkey=key)
        print("[✅] SFTP connection established.")
        sftp = ssh.open_sftp()

        # Step 02 : List all files in the remote directory
        files = sftp.listdir_attr(SFTP_REMOTE_DIR)
        print(files)
        
        print(f"[INFO] Total files found: {len(files)}")

        # Step 03 : Process each file
        for f in files:
            # Step 04 : Skip non-regular/hidden files
            if f.filename.startswith(".") or not stat.S_ISREG(f.st_mode):
                print(f"[SKIP] Non-regular/hidden file: {f.filename}")
                continue

            # Step 05 : Extract file name and modified time
            file_name = f.filename
            file_mtime = datetime.utcfromtimestamp(f.st_mtime)
            prefix = file_name[:2].strip()

            print("-" * 60)
            print(f"[{processed + 1}] Considering file: {file_name}")
            print(f"   ➤ Prefix: {prefix}")
            print(f"   ➤ File modified time: {file_mtime}")

            # Step 06 : Check if the file is already processed
            last_for_file = file_uploads_col.find_one(
                {"fileName": file_name},
                sort=[("createdAt", -1)]
            )

            if (last_for_file and 
                last_for_file.get("status") == "Completed" and 
                file_mtime <= last_for_file.get("fileMtime", datetime.min)):
                print(f"[SKIP] File {file_name} already completed and not modified since last process.")
                continue

            processed += 1

            print(f"[INFO] Proceeding to process/retry {file_name}")

            # Step 07 : Check if the checklist mapping exists
            try:
                checklist_ref = ensure_prefix_in_db(prefix)
                if not checklist_ref:
                    print(f"[⚠️] No checklist mapping for prefix {prefix}, skipping.")
                    continue

                remote_path = f"{SFTP_REMOTE_DIR}/{file_name}"
                local_path = os.path.join(TEMP_DOWNLOAD_DIR, file_name)

                doc_id = None
                inspection_ref = None

                if last_for_file and last_for_file.get("status") != "Completed":
                    # Reuse and update the existing non-completed record (failed or pending)
                    doc_id = last_for_file["_id"]
                    inspection_ref = last_for_file["inspectionRef"]
                    print(f"[INFO] Reusing existing non-completed record (ID: {doc_id}) for {file_name}.")

                    # Download file temporarily
                    print(f"[INFO] Downloading to: {local_path}")
                    sftp.get(remote_path, local_path)
                    print("[✅] Download complete.")

                    # Update the existing document
                    update_fields = {
                        "$set": {
                            "status": "Pending",
                            "filePath": local_path,
                            "inspectionDate": datetime.utcnow().isoformat(timespec="milliseconds") + "Z",
                            "fileMtime": file_mtime,
                            "updatedAt": datetime.utcnow(),
                        }
                    }
                    file_uploads_col.update_one({"_id": doc_id}, update_fields)
                    print("[✅] Existing document updated in checklistfileuploads.")
                else:
                    # Create new inspection and document
                    inspection_ref = create_system_inspection(company_ref, facility_ref, checklist_ref)

                    # Download file temporarily
                    print(f"[INFO] Downloading to: {local_path}")
                    sftp.get(remote_path, local_path)
                    print("[✅] Download complete.")

                    # Save new record in DB
                    doc = {
                        "checklistRef": ObjectId(checklist_ref),
                        "inspectionRef": ObjectId(inspection_ref),
                        "inspectionDate": datetime.utcnow().isoformat(timespec="milliseconds") + "Z",
                        "filePath": local_path,
                        "status": "Pending",
                        "userinfo": ObjectId(userinfo),
                        "companyRef": ObjectId(company_ref),
                        "facilityRef": ObjectId(facility_ref),
                        "isBulkSystemUpload": True,
                        "source": "System",
                        "lastRecord": 0,
                        "createdAt": datetime.utcnow(),
                        "updatedAt": datetime.utcnow(),
                        "__v": 0,
                        "fileName": file_name,
                        "fileMtime": file_mtime
                    }
                    result = file_uploads_col.insert_one(doc)
                    doc_id = result.inserted_id
                    print(f"[✅] New document saved to checklistfileuploads with ID: {doc_id}")

                # Process CSV
                process_csv_file(local_path)

                # Assume no exception means success; update status to Completed
                file_uploads_col.update_one(
                    {"_id": doc_id},
                    {"$set": {"status": "Completed", "updatedAt": datetime.utcnow()}}
                )
                print("[✅] Processing completed; status updated to Completed.")

                # Delete the file after processing
                if os.path.exists(local_path):
                    os.remove(local_path)
                    print(f"[INFO] Temporary file deleted: {local_path}")

                # Update lastUpdatedAt in checklistmaps for this prefix
                update_result = checklist_map_col.update_one(
                    {"acronym": {"$regex": f"^{prefix.strip()}$", "$options": "i"}},
                    {"$set": {"lastUpdatedAt": datetime.utcnow()}}
                )
                if update_result.matched_count > 0:
                    print(f"[INFO] Updated lastUpdatedAt for prefix '{prefix}' in checklistmaps.")
                else:
                    print(f"[WARN] No matching document found to update lastUpdatedAt for prefix '{prefix}'.")

                # Send email notification after successful processing
                subject = f"File Processing Completed: {file_name}"
                body = f"The file '{file_name}' has been successfully downloaded, processed, and uploaded to the database.\n\nDetails:\n- Prefix: {prefix}\n- Modified Time: {file_mtime}\n- Inspection ID: {inspection_ref}\n- Checklist ID: {checklist_ref}\n- Upload ID: {doc_id}"
                send_email(subject, body)

                successful += 1

            except Exception as file_err:
                print(f"[❌] Error processing {file_name}: {file_err}")

                # Update status to Failed if doc_id exists
                if doc_id:
                    file_uploads_col.update_one(
                        {"_id": doc_id},
                        {"$set": {"status": "Failed", "updatedAt": datetime.utcnow()}}
                    )
                    print(f"[INFO] Updated status to Failed for upload ID: {doc_id}")

                # Clean up temp file if exists
                if os.path.exists(local_path):
                    os.remove(local_path)
                    print(f"[INFO] Temporary file deleted: {local_path}")

                # Send email notification on failure
                subject = f"File Processing Failed: {file_name}"
                body = f"An error occurred while processing the file '{file_name}'.\n\nError: {str(file_err)}\n\nDetails:\n- Prefix: {prefix}\n- Modified Time: {file_mtime}"
                send_email(subject, body)

                failed += 1

        print("=" * 60)
        print(
            f"[SUMMARY] Processed: {processed}, Successful: {successful}, Failed: {failed}")

        sftp.close()
        ssh.close()
        print("[✅] SFTP connection closed.")

    except Exception as e:
        print(f"[❌] SFTP Process Error: {e}")