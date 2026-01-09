"""Database operations."""
from datetime import datetime
from bson import ObjectId
from dateutil.parser import parse as date_parse
from config.settings import USER_INFO
from services.database.collections import checklist_map_col, inspections_col, file_uploads_col, checklistresults_col


def ensure_prefix_in_db(prefix: str):
    """
    Check if checklist mapping exists for a given prefix.
    
    :param prefix: Two-character prefix from filename
    :return: checklistRef if found, None otherwise
    """
    mapping = checklist_map_col.find_one({
        "acronym": {"$regex": f"^{prefix.strip()}$", "$options": "i"}
    })
    if mapping:
        return mapping.get("checklistRef")
    else:
        return None


def find_completed_inspection(company_ref: str, facility_ref: str, checklist_ref: str):
    """
    Find an existing completed file upload with the same checklistRef in checklistfileuploads.
    Returns its inspectionRef and the lastRecord count if found.
    
    :param company_ref: Company reference ID
    :param facility_ref: Facility reference ID
    :param checklist_ref: Checklist reference ID
    :return: Tuple (inspection_id: str | None, last_record: int)
    """
    existing_upload = file_uploads_col.find_one({
        "companyRef": ObjectId(company_ref),
        "facilityRef": ObjectId(facility_ref),
        "checklistRef": ObjectId(checklist_ref),
        "status": "Completed",
        "isBulkSystemUpload": True  # Only for system uploads
    }, sort=[("createdAt", -1)])  # Get the most recent one
    
    if existing_upload and existing_upload.get("status") == "Completed":
        inspection_id = str(existing_upload["inspectionRef"])
        last_record = int(existing_upload.get("lastRecord", 0) or 0)
        return inspection_id, last_record
    else:
        return None, 0


def create_system_inspection(company_ref: str, facility_ref: str, checklist_ref: str) -> str:
    """
    Create a new system inspection.
    
    :param company_ref: Company reference ID
    :param facility_ref: Facility reference ID
    :param checklist_ref: Checklist reference ID
    :return: Inspection ID as string
    """
    inspection_payload = {
        "status": "Pending",
        "isActive": True,
        "isUnSchedule": True,
        "isDeleted": False,
        "companyRef": ObjectId(company_ref),
        "facilityRef": ObjectId(facility_ref),
        "checklistRef": ObjectId(checklist_ref),
        "assignee": [USER_INFO],
        "followers": [],
        "createdAt": datetime.utcnow(),
        "updatedAt": datetime.utcnow(),
        "__v": 0
    }
    result = inspections_col.insert_one(inspection_payload)
    return str(result.inserted_id)


def get_last_file_upload(file_name: str):
    """Get the last file upload record for a given filename."""
    return file_uploads_col.find_one(
        {"fileName": file_name},
        sort=[("createdAt", -1)]
    )


def create_file_upload_record(file_data: dict):
    """Create a new file upload record."""
    result = file_uploads_col.insert_one(file_data)
    return result.inserted_id


def update_file_upload_status(doc_id, status: str, **kwargs):
    """Update file upload status and other fields."""
    update_fields = {"$set": {"status": status, "updatedAt": datetime.utcnow(), **kwargs}}
    file_uploads_col.update_one({"_id": doc_id}, update_fields)


def update_checklist_map_last_updated(prefix: str):
    """Update lastUpdatedAt in checklistmaps for a given prefix."""
    checklist_map_col.update_one(
        {"acronym": {"$regex": f"^{prefix.strip()}$", "$options": "i"}},
        {"$set": {"lastUpdatedAt": datetime.utcnow()}}
    )


def deactivate_file_uploads_by_checklist_ref(company_ref: str, facility_ref: str, checklist_ref: str):
    """
    Deactivate all file uploads with the same checklistRef (prefix) by setting isInspectionActive to False.
    
    :param company_ref: Company reference ID
    :param facility_ref: Facility reference ID
    :param checklist_ref: Checklist reference ID (derived from prefix)
    :return: Number of documents modified
    """
    # Validate checklist_ref before converting to ObjectId
    if not checklist_ref:
        return 0
    
    try:
        checklist_obj_id = ObjectId(checklist_ref)
    except (TypeError, ValueError):
        # Invalid ObjectId format, return 0 without raising exception
        return 0
    
    result = file_uploads_col.update_many(
        {
            "companyRef": ObjectId(company_ref),
            "facilityRef": ObjectId(facility_ref),
            "checklistRef": checklist_obj_id,
            "isBulkSystemUpload": True,
            "source": "System"  # Only for System source
        },
        {
            "$set": {
                "isInspectionActive": False,
                "updatedAt": datetime.utcnow()
            }
        }
    )
    return result.modified_count


def ensure_only_latest_active_by_checklist_ref(company_ref: str, facility_ref: str, checklist_ref: str):
    """
    Ensure only the most recent completed file upload with the same checklistRef (prefix) 
    has isInspectionActive=True. All older ones are set to False.
    Only applies to records with source: "System".
    
    :param company_ref: Company reference ID
    :param facility_ref: Facility reference ID
    :param checklist_ref: Checklist reference ID (derived from prefix)
    :return: Tuple (most_recent_doc_id, deactivated_count)
    """
    # Validate checklist_ref before converting to ObjectId
    if not checklist_ref:
        return None, 0
    
    try:
        checklist_obj_id = ObjectId(checklist_ref)
    except (TypeError, ValueError):
        # Invalid ObjectId format, return without raising exception
        return None, 0
    
    # Find the most recent completed file upload with this checklistRef
    most_recent = file_uploads_col.find_one(
        {
            "companyRef": ObjectId(company_ref),
            "facilityRef": ObjectId(facility_ref),
            "checklistRef": checklist_obj_id,
            "isBulkSystemUpload": True,
            "source": "System",
            "status": "Completed"
        },
        sort=[("createdAt", -1)]  # Most recent first
    )
    
    if not most_recent:
        return None, 0
    
    most_recent_id = most_recent["_id"]
    
    # Deactivate all other completed file uploads with the same checklistRef
    result = file_uploads_col.update_many(
        {
            "companyRef": ObjectId(company_ref),
            "facilityRef": ObjectId(facility_ref),
            "checklistRef": checklist_obj_id,
            "isBulkSystemUpload": True,
            "source": "System",
            "status": "Completed",
            "_id": {"$ne": most_recent_id}  # Exclude the most recent one
        },
        {
            "$set": {
                "isInspectionActive": False,
                "updatedAt": datetime.utcnow()
            }
        }
    )
    
    # Ensure the most recent one is active
    file_uploads_col.update_one(
        {"_id": most_recent_id},
        {
            "$set": {
                "isInspectionActive": True,
                "updatedAt": datetime.utcnow()
            }
        }
    )
    
    return most_recent_id, result.modified_count


def update_inspection_updated_at(inspection_ref: str):
    """
    Update the updatedAt field in inspections collection to current time.
    
    :param inspection_ref: Inspection reference ID (string or ObjectId)
    """
    inspection_id = ObjectId(inspection_ref) if isinstance(inspection_ref, str) else inspection_ref
    
    inspections_col.update_one(
        {"_id": inspection_id},
        {
            "$set": {
                "updatedAt": datetime.utcnow()
            }
        }
    )


def update_inspection_completed(inspection_ref: str, user_info: str):
    """
    Update inspection status to 'completed' and set assignee to user_info.
    
    :param inspection_ref: Inspection reference ID (string or ObjectId)
    :param user_info: User info ID (string or ObjectId)
    """
    inspection_id = ObjectId(inspection_ref) if isinstance(inspection_ref, str) else inspection_ref
    user_id = ObjectId(user_info) if isinstance(user_info, str) else user_info
    
    inspections_col.update_one(
        {"_id": inspection_id},
        {
            "$set": {
                "status": "Completed",
                "assignee": [user_id],
                "updatedAt": datetime.utcnow()
            }
        }
    )


def create_checklistresult_if_not_exists(inspection_ref: str, checklist_ref: str = None, source: str = None):
    """
    Create a new checklistresult document for the given inspectionRef if one doesn't exist.
    Only one document should exist per inspectionRef.
    
    :param inspection_ref: Inspection reference ID (string or ObjectId)
    :param checklist_ref: Checklist reference ID (string or ObjectId) - required for System source
    :param source: Source of the upload ("System" or "UI")
    """
    inspection_id = ObjectId(inspection_ref) if isinstance(inspection_ref, str) else inspection_ref
    
    # Check if a checklistresult already exists for this inspectionRef
    existing_result = checklistresults_col.find_one({"inspectionRef": inspection_id})
    
    if existing_result:
        return
    
    # Get file upload record to extract necessary fields
    file_upload = file_uploads_col.find_one(
        {"inspectionRef": inspection_id, "status": "Completed"},
        sort=[("createdAt", -1)]
    )
    
    if not file_upload:
        return
    
    # If source is System and checklist_ref is provided, set isInProgress=False for existing System checklistresults with same checklistRef
    if source == "System" and checklist_ref:
        try:
            checklist_obj_id = ObjectId(checklist_ref) if isinstance(checklist_ref, str) else checklist_ref
            
            # First check if there are any documents to update (faster than update_many if none exist)
            existing_count = checklistresults_col.count_documents({
                "checklistRef": checklist_obj_id,
                "source": "System",
                "isInProgress": True
            })
            
            # Only update if there are documents to update
            if existing_count > 0:
                result = checklistresults_col.update_many(
                    {
                        "checklistRef": checklist_obj_id,
                        "source": "System",
                        "isInProgress": True
                    },
                    {
                        "$set": {
                            "isInProgress": False,
                            "updatedAt": datetime.utcnow()
                        }
                    }
                )
        except (TypeError, ValueError):
            # Invalid ObjectId, continue without deactivating
            pass
    
    # Parse inspectionDate
    inspection_date = file_upload.get("inspectionDate")
    if isinstance(inspection_date, str):
        try:
            inspection_date = date_parse(inspection_date)
        except Exception:
            inspection_date = datetime.utcnow()
    elif not inspection_date:
        inspection_date = datetime.utcnow()
    
    # Create new checklistresult document
    now = datetime.utcnow()
    checklistresult_doc = {
        "status": "Completed",
        "totalScore": 0,
        "achivScore": 0,
        "scorePer": 0,
        "isActive": True,
        "isScoreUpdate": False,
        "isDeleted": False,
        "isQRupload": False,
        "isEvent": False,
        "isInProgressEmailSent": False,
        "isInProgress": True,
        "checklistRef": ObjectId(file_upload["checklistRef"]),
        "inspectionRef": inspection_id,
        "inspectionDate": inspection_date,
        "score": 0,
        "publicloginFirstName": "",
        "publicloginLastName": "",
        "publicloginEmail": "",
        "userRef": ObjectId(file_upload["userinfo"]),
        "createdAt": now,
        "updatedAt": now,
        "__v": 0
    }
    
    # Add source field if it's System
    if source == "System":
        checklistresult_doc["source"] = "System"
    
    checklistresults_col.insert_one(checklistresult_doc)

