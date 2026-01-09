"""Main file processing orchestration."""
import os
import stat
from datetime import datetime
from bson import ObjectId
from process_csv import process_csv_file
from config.settings import COMPANY_REF, FACILITY_REF, USER_INFO, TEMP_DOWNLOAD_DIR
from services.sftp.connection import SFTPConnection
from services.sftp.file_handler import SFTPFileHandler
from services.database.operations import (
    ensure_prefix_in_db,
    create_system_inspection,
    get_last_file_upload,
    create_file_upload_record,
    update_file_upload_status,
    update_checklist_map_last_updated,
    create_checklistresult_if_not_exists
)
from services.processing.date_utils import parse_start_date, extract_date_from_filename
from services.email.sender import send_email
from helpers.logger import logger


def fetch_files_from_sftp():
    """Main function to fetch and process files from SFTP."""
    processed = successful = failed = 0

    # Step 1: List files (open connection, list, close immediately)
    try:
        with SFTPConnection() as sftp_conn:
            file_handler = SFTPFileHandler(sftp_conn.sftp)
            files = file_handler.list_files()
    except Exception as e:
        logger.error(f"SFTP Connection Error while listing files: {e}")
        return

    # Step 2: Process each file (reuse single connection for all downloads in this run)
    download_conn = None
    download_handler = None
    
    try:
        for f in files:
            # Extract file name and modified time
            file_name = f.filename
            file_mtime = datetime.utcfromtimestamp(f.st_mtime)
            prefix = file_name[:2].strip()

            # Skip non-regular/hidden files (check using file attributes from list - no connection needed)
            if file_name.startswith(".") or not stat.S_ISREG(f.st_mode):
                logger.info(f"Skipping {file_name}: File is hidden or not a regular file")
                continue

            # Check if file date is before START_DATE (files from START_DATE onwards should be processed)
            start_date = parse_start_date()
            if start_date:
                file_date = extract_date_from_filename(file_name)
                if file_date:
                    # Normalize file_date to start of day for comparison
                    file_date_normalized = file_date.replace(hour=0, minute=0, second=0, microsecond=0)
                    # Skip files before START_DATE (process files from START_DATE onwards)
                    if file_date_normalized < start_date:
                        logger.info(f"Skipping {file_name}: File date ({file_date_normalized.strftime('%Y-%m-%d')}) is before START_DATE ({start_date.strftime('%Y-%m-%d')})")
                        continue
                else:
                    logger.info(f"Skipping {file_name}: Could not extract date from filename")

            # Check if the file is already processed
            last_for_file = get_last_file_upload(file_name)

            if (last_for_file and
                last_for_file.get("status") == "Completed" and
                file_mtime <= last_for_file.get("fileMtime", datetime.min)):
                logger.info(f"Skipping {file_name}: File already completed and not modified since last process (last modified: {file_mtime}, last processed: {last_for_file.get('fileMtime', datetime.min)})")
                continue

            processed += 1
            logger.info(f"Processing {file_name}")

            # Check if the checklist mapping exists
            checklist_ref = ensure_prefix_in_db(prefix)
            if not checklist_ref:
                logger.info(f"Skipping {file_name}: No checklist mapping found for prefix '{prefix}'")
                continue

            local_path = os.path.join(TEMP_DOWNLOAD_DIR, file_name)
            doc_id = None
            inspection_ref = None

            try:
                # Always create a new inspection for each file (separate inspections)
                inspection_ref = create_system_inspection(COMPANY_REF, FACILITY_REF, checklist_ref)
                logger.info(f"Processing {file_name}: Created new inspection (ID: {inspection_ref})")

                # Download file (reuse connection if available, create if needed)
                try:
                    # Create connection if it doesn't exist or was closed
                    if download_conn is None or download_handler is None:
                        download_conn = SFTPConnection()
                        download_conn.connect()
                        download_handler = SFTPFileHandler(download_conn.sftp)
                        logger.info("Opened SFTP connection for file downloads")
                    
                    download_handler.download_file(file_name, local_path)
                    
                    # Verify file was downloaded successfully
                    if not os.path.exists(local_path):
                        raise FileNotFoundError(f"File was not downloaded to {local_path}")
                    
                    # Verify file is not empty
                    if os.path.getsize(local_path) == 0:
                        raise ValueError(f"Downloaded file {local_path} is empty")
                    
                    logger.info(f"Downloaded {file_name} from SFTP to {local_path} ({os.path.getsize(local_path)} bytes)")
                    
                    # Close connection before CSV processing (which can take a long time)
                    # This prevents holding connection during long CSV processing
                    if download_conn:
                        download_conn.close()
                        download_conn = None
                        download_handler = None
                except Exception as download_err:
                    logger.error(f"Error downloading {file_name} from SFTP: {download_err}")
                    # Clean up partial download if exists
                    if os.path.exists(local_path):
                        try:
                            os.remove(local_path)
                            logger.info(f"Removed partial download: {local_path}")
                        except Exception as cleanup_err:
                            logger.warning(f"Could not remove partial download {local_path}: {cleanup_err}")
                    # Close connection on error to force reconnect on next file
                    if download_conn:
                        try:
                            download_conn.close()
                        except:
                            pass
                    download_conn = None
                    download_handler = None
                    raise

                # Save new record in DB - each file starts fresh
                doc = {
                    "checklistRef": ObjectId(checklist_ref),
                    "inspectionRef": ObjectId(inspection_ref),
                    "inspectionDate": datetime.utcnow().isoformat(timespec="milliseconds") + "Z",
                    "filePath": local_path,
                    "status": "Pending",
                    "userinfo": ObjectId(USER_INFO),
                    "companyRef": ObjectId(COMPANY_REF),
                    "facilityRef": ObjectId(FACILITY_REF),
                    "isBulkSystemUpload": True,
                    "source": "System",
                    # Each file starts from 0 (separate inspections, no continuation)
                    "orderStartBase": 0,
                    "processedRows": 0,
                    "lastRecord": 0,
                    "createdAt": datetime.utcnow(),
                    "updatedAt": datetime.utcnow(),
                    "__v": 0,
                    "fileName": file_name,
                    "fileMtime": file_mtime
                }
                doc_id = create_file_upload_record(doc)
                logger.info(f"Processing {file_name}: Created file upload record (ID: {doc_id})")

                # Process CSV (no SFTP connection needed - connection already closed)
                logger.info(f"Processing {file_name}: Starting CSV processing")
                process_csv_file(local_path)
                logger.info(f"Processing {file_name}: CSV processing completed")

                # Update status to Completed
                update_file_upload_status(doc_id, "Completed")

                # Create checklistresult document if it doesn't exist
                # Pass checklist_ref and source="System" to handle isInProgress logic
                create_checklistresult_if_not_exists(inspection_ref, checklist_ref, "System")
                
                # Delete the file after processing
                if os.path.exists(local_path):
                    os.remove(local_path)

                # Update lastUpdatedAt in checklistmaps for this prefix
                update_checklist_map_last_updated(prefix)

                # Send email notification after successful processing
                subject = f"File Processing Completed: {file_name}"
                body = (f"The file '{file_name}' has been successfully downloaded, processed, and uploaded to the database.\n\n"
                       f"Details:\n- Prefix: {prefix}\n- Modified Time: {file_mtime}\n- Inspection ID: {inspection_ref}\n"
                       f"- Checklist ID: {checklist_ref}\n- Upload ID: {doc_id}")
                send_email(subject, body)

                logger.info(f"Processed {file_name}")
                successful += 1

            except Exception as file_err:
                import traceback
                error_details = traceback.format_exc()
                logger.error(f"Error processing {file_name} (Prefix: {prefix}, Inspection: {inspection_ref if inspection_ref else 'N/A'}, Doc ID: {doc_id if doc_id else 'N/A'}): {str(file_err)}")
                logger.error(f"Error details for {file_name}:\n{error_details}")

                # Update status to Failed if doc_id exists
                if doc_id:
                    update_file_upload_status(doc_id, "Failed")
                    logger.info(f"Updated status to Failed for {file_name} (Doc ID: {doc_id})")

                # Clean up temp file if exists
                if os.path.exists(local_path):
                    os.remove(local_path)
                    logger.info(f"Cleaned up temporary file for {file_name}: {local_path}")

                # Send email notification on failure
                subject = f"File Processing Failed: {file_name}"
                body = (f"An error occurred while processing the file '{file_name}'.\n\n"
                       f"Error: {str(file_err)}\n\nDetails:\n- Prefix: {prefix}\n- Modified Time: {file_mtime}")
                send_email(subject, body)

                failed += 1
    
    finally:
        # Close SFTP connection after all downloads are complete
        if download_conn:
            try:
                download_conn.close()
                logger.info("Closed SFTP connection after all downloads")
            except Exception as e:
                logger.warning(f"Error closing SFTP connection: {e}")


def check_sftp():
    """Check SFTP connection."""
    try:
        with SFTPConnection() as sftp_conn:
            file_handler = SFTPFileHandler(sftp_conn.sftp)
            files = file_handler.list_files()
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        logger.error(f"SFTP Check Error: {str(e)}")
        logger.error(f"SFTP Check Error Details:\n{error_details}")

