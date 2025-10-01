import os
import time
import stat
from datetime import datetime
import paramiko
from bson import ObjectId
from pymongo import MongoClient
from dotenv import load_dotenv
from process_csv import process_csv_file

# ── Load environment variables ────────────────────────────────────────────────
load_dotenv()

SFTP_HOST = os.getenv("SFTP_HOST")
SFTP_USER = os.getenv("SFTP_USER")
SFTP_KEY_PATH = os.getenv("SFTP_KEY_PATH")
SFTP_REMOTE_DIR = os.getenv("SFTP_REMOTE_DIR")
MONGO_URI = os.getenv("DB_URL")
TEMP_DOWNLOAD_DIR = os.getenv("FILE_PATH")  # temporary download folder

if not all([SFTP_HOST, SFTP_USER, SFTP_KEY_PATH, SFTP_REMOTE_DIR, MONGO_URI]):
    raise RuntimeError(
        "❌ Missing one or more required environment variables in .env")

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




# ── Main SFTP processing function ─────────────────────────────────────────────


def fetch_files_from_sftp():
    print("=" * 60)
    print(
        f"[fetch_files_from_sftp] Cron triggered at: {datetime.utcnow().isoformat()}")
    print("=" * 60)

    key = paramiko.RSAKey.from_private_key_file(SFTP_KEY_PATH)
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    processed = successful = failed = 0
    company_ref = "68ca635393105c0c3fcd8b0e"
    facility_ref = "68ca635393105c0c3fcd8b11"
    userinfo = "68ca635393105c0c3fcd8b14"
    
    # Get the most recent createdAt from checklistfileuploads collection
    last_upload_doc = file_uploads_col.find_one({}, sort=[("createdAt", -1)])
    last_updated = last_upload_doc["createdAt"] if last_upload_doc else datetime.min

    print(f"[INFO] Most recent upload time: {last_updated}")

    try:
        print(f"[INFO] Connecting to SFTP: {SFTP_HOST}")
        ssh.connect(hostname=SFTP_HOST, username=SFTP_USER, pkey=key)
        print("[✅] SFTP connection established.")
        sftp = ssh.open_sftp()

        files = sftp.listdir_attr(SFTP_REMOTE_DIR)
        print(files)
        
        print(f"[INFO] Total files found: {len(files)}")

        for f in files:
            if f.filename.startswith(".") or not stat.S_ISREG(f.st_mode):
                print(f"[SKIP] Non-regular/hidden file: {f.filename}")
                continue

            file_name = f.filename
            file_mtime = datetime.utcfromtimestamp(f.st_mtime)
            
            if file_mtime <= last_updated:
                print(f"[SKIP] File {file_name} not modified after last upload time ({last_updated}).")
                continue

            processed += 1
            remote_path = f"{SFTP_REMOTE_DIR}/{file_name}"
            prefix = file_name[:2].strip()

            print("-" * 60)
            print(f"[{processed}] Processing file: {file_name}")
            print(f"   ➤ Prefix: {prefix}")
            print(f"   ➤ File modified time: {file_mtime}")

            try:
                # Check if record already exists
                existing = file_uploads_col.find_one(
                    {"filePath": {"$regex": f"/{prefix}", "$options": "i"}},
                    sort=[("createdAt", -1)]
                )

                if 1 != 1:
                    print("[INFO] Existing file found, reusing references.")
                    inspection_ref = existing["inspectionRef"]
                    checklist_ref = existing["checklistRef"]
                else:
                    checklist_ref = ensure_prefix_in_db(prefix)
                    if not checklist_ref:
                        print(
                            f"[⚠️] No checklist mapping for prefix {prefix}, skipping.")
                        continue

                    inspection_ref = create_system_inspection(
                        company_ref, facility_ref, checklist_ref
                    )

                # Download file temporarily
                local_path = os.path.join(TEMP_DOWNLOAD_DIR, file_name)
                print(f"[INFO] Downloading to: {local_path}")
                sftp.get(remote_path, local_path)
                print("[✅] Download complete.")

                # Save record in DB
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
                    "__v":0,
                    "fileName": file_name
                }
                file_uploads_col.insert_one(doc)
                print("[✅] Document saved to checklistfileuploads.")

                # Process CSV
                process_csv_file(local_path)

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

                successful += 1

            except Exception as file_err:
                print(f"[❌] Error processing {file_name}: {file_err}")
                failed += 1

            # print("[INFO] Waiting 60 seconds before next file...")
            # time.sleep(60)

        print("=" * 60)
        print(
            f"[SUMMARY] Processed: {processed}, Successful: {successful}, Failed: {failed}")

        sftp.close()
        ssh.close()
        print("[✅] SFTP connection closed.")

    except Exception as e:
        print(f"[❌] SFTP Process Error: {e}")