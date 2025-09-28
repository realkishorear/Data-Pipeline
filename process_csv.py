import csv
import math
import os
import re
import time
import logging
import resource
from datetime import datetime
from bson import ObjectId
from dateutil.parser import parse, ParserError
from pymongo import MongoClient, UpdateOne, ReturnDocument
from helpers.apis import find_one, schedule_inspection_open, inspection_completed
from helpers.dateTime import check_date_and_time

# ---------------------------------------------------------------------------
# Logging configuration
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,   # Change to DEBUG for very detailed trace
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


def heap_usage():
    """Log current process heap usage (Linux/macOS)."""
    usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024  # MB
    logger.info(f"📊 Heap usage: {usage:.2f} MB")

# ---------------------------------------------------------------------------


def process_csv_file(local_csv_path: str):
    """
    Process a single CSV file and update Mongo collections accordingly.
    """

    def normalize_header(text):
        text = str(text or "")
        text = re.sub(r"<[^>]+>", "", text)  # remove HTML
        return text.strip().lower()

    logger.info(f"▶️ Starting processing for file: {local_csv_path}")

    client = MongoClient(os.getenv("DB_URL"))
    db = client.get_default_database()

    checklist_file_upload_collection = db["checklistfileuploads"]
    checklist_inspection_collection = db["checklistinspections"]
    inspection_collection = db["inspections"]

    file_doc = checklist_file_upload_collection.find_one(
        {"filePath": local_csv_path})
    if not file_doc:
        logger.warning(f"No file doc found for path: {local_csv_path}")
        return

    logger.info(f"Found file document with _id={file_doc['_id']}")

    try:
        updated_file = checklist_file_upload_collection.find_one_and_update(
            {"_id": file_doc["_id"], "status": "Pending"},
            {"$set": {"status": "Processing"}},
            return_document=ReturnDocument.AFTER
        )

        if not updated_file:
            logger.info(
                f"File {file_doc['_id']} already processed or not Pending. Skipping.")
            return

        process_model = updated_file
        logger.debug(f"Process model loaded: {process_model}")

        # Ensure source
        if (src := process_model.get("source")) not in ("System", "UI"):
            checklist_file_upload_collection.update_one(
                {"_id": file_doc["_id"]}, {"$set": {"source": "UI"}}
            )
            logger.info(f"Updated file source to UI for {file_doc['_id']}")

        # 2️⃣ Load checklist questions
        logger.info("Fetching checklist questions via API...")
        check_data = find_one(process_model["checklistRef"])
        if "error" in check_data:
            raise ValueError(
                f"Failed to fetch checklist: {check_data['message']}")
        question_list = []
        for page in check_data.get("data", {}).get("page", []):
            for section in page.get("sections", []):
                for q in section.get("questions", []):
                    question_list.append(q)
        logger.info(f"Loaded {len(question_list)} questions from checklist")

        # 3️⃣ Ensure inspectionRef
        if not process_model.get("inspectionRef"):
            logger.info("Scheduling inspection (API call)...")
            inspection = schedule_inspection_open(process_model)
            if "error" in inspection:
                raise ValueError(
                    f"Failed to schedule inspection: {inspection['message']}")
            process_model["inspectionRef"] = inspection["data"]["_id"]
            process_model["inspectionDate"] = inspection["data"]["inspectionDate"]
            logger.info(
                f"Inspection scheduled: {process_model['inspectionRef']}")

        # 4️⃣ Setup batching
        bulk_ops = []
        MAX_BULK_OPS = 10000
        common_id_map = {
            f"{q['type']}-{normalize_header(q['title'])}": ObjectId() for q in question_list
        }

        question_meta = [
            {
                **q,
                "cleanTitle": normalize_header(q["title"]),
                "commonId": common_id_map[f"{q['type']}-{normalize_header(q['title'])}"]
            }
            for q in question_list
        ]

        logger.debug(f"Prepared metadata for {len(question_meta)} questions")

        # Free memory
        question_list.clear()
        heap_usage()

        start_order = 0
        max_order_doc = checklist_inspection_collection.find_one(
            {
                "checklistRef": process_model["checklistRef"],
                "inspectionRef": process_model["inspectionRef"],
            },
            sort=[("order", -1)],
            projection={"order": 1}
        )
        if max_order_doc and "order" in max_order_doc:
            start_order = int(max_order_doc["order"]) + 1
        logger.info(f"Starting order index at {start_order}")

        row_index = 0

        def process_row(row_values, headers, bulk_ops):
            nonlocal row_index
            row_order = row_index + start_order
            row_index += 1

            if row_index % 1000 == 0:
                logger.info(f"Processed {row_index} rows so far...")

            if row_index == 1:
                logger.debug(f"CSV Headers raw: {headers}")
                logger.debug(
                    f"CSV Headers normalized: {[normalize_header(h) for h in headers]}")

            userid = process_model["userinfo"]

            for i, q in enumerate(question_meta):
                value = row_values[i].strip() if i < len(row_values) else None  # Added strip to match TS trim
                answer_obj = {
                    "_id": q["_id"],
                    "isHide": q["isHide"],
                    "type": q["type"],
                    "title": q["title"],
                    "checklistRef": q["checklistRef"],
                    "sectionRef": q["sectionRef"],
                    "createdAt": q["createdAt"],
                    "updatedAt": q["updatedAt"],
                    "__v": q["__v"],
                    "checklistQuestionDetailsRef": q["checklistQuestionDetailsRef"],
                    "checklistQuestionRef": q["checklistQuestionRef"],
                    "indexOrder": q["indexOrder"],
                    "details": q.get("details", []),
                    "qtype": q["qtype"],
                    "scorevalue": 0,
                    "answer": None,
                    "scoring": q["scoring"],
                    "isDate": q["isDate"],
                    "isTime": q["isTime"],
                    "isSignature": q["isSignature"],
                    "mandatory": q["mandatory"],
                    "isAddnotes": q["isAddnotes"],
                    "ismultiselectdropdown": q["ismultiselectdropdown"],
                }
                # Value processing
                if value not in (None, ""):
                    try:
                        if q["type"] == "Single choice responder":
                            ans_single = next(
                                (r for r in q.get("answerOptions", [])
                                 if normalize_header(r["name"]) == normalize_header(value)),
                                None
                            )
                            if ans_single:
                                answer_obj["scorevalue"] = float(
                                    ans_single.get("score", 0)) or 0
                            answer_obj["answer"] = value

                        elif q["type"] == "Multiple choice responder":
                            choices = [v.strip()
                                       for v in str(value).split(",")]
                            score = 0
                            for c in choices:
                                ans_multi = next(
                                    (o for o in q.get("answerOptions", [])
                                     if normalize_header(o["name"]) == normalize_header(c)),
                                    None
                                )
                                if ans_multi:
                                    score += float(ans_multi.get("score", 0)) or 0
                            answer_obj["scorevalue"] = score
                            answer_obj["answer"] = choices

                        elif q["type"] == "Text answer":
                            score = 0
                            for sc in q.get("scoreOptions", []):
                                if (
                                    (sc["condition"] == "is customized keyword" and sc.get(
                                        "count") == value)
                                    or (sc["condition"] == "is not blank" and value)
                                    or (sc["condition"] == "is blank" and not value)
                                ):
                                    score += float(sc.get("score", 0)) or 0
                            answer_obj["scorevalue"] = score
                            answer_obj["answer"] = value

                        elif q["type"] == "Date & Time":
                            date_output = check_date_and_time(value)
                            if date_output["isValidDate"]:
                                answer_obj["answer"] = {
                                    "date": date_output["date"]}

                        elif q["type"] in ["Slider", "Number"]:
                            try:
                                num_value = float(value)
                            except ValueError:
                                num_value = math.nan
                            score = 0
                            for sc in q.get("scoreOptions", []):
                                try:
                                    cnt = float(sc["count"])
                                except (ValueError, KeyError):
                                    cnt = 0
                                cond = sc["condition"]
                                if cond == "less than" and num_value < cnt:
                                    score += float(sc.get("score", 0)) or 0
                                elif cond == "less than or equal to" and num_value <= cnt:
                                    score += float(sc.get("score", 0)) or 0
                                elif cond == "equal to" and num_value == cnt:
                                    score += float(sc.get("score", 0)) or 0
                                elif cond == "not equal to" and num_value != cnt:
                                    score += float(sc.get("score", 0)) or 0
                                elif cond == "greater than or equal to" and num_value >= cnt:
                                    score += float(sc.get("score", 0)) or 0
                                elif cond == "greater than" and num_value > cnt:
                                    score += float(sc.get("score", 0)) or 0
                            answer_obj["scorevalue"] = score
                            answer_obj["answer"] = value

                        else:
                            answer_obj["answer"] = value
                    except Exception:
                        logger.exception(
                            f"Failed to process value for question {q.get('title')}")

                bulk_ops.append(
                    UpdateOne(
                        {
                            "inspectionRef": process_model["inspectionRef"],
                            "checklistRef": process_model["checklistRef"],
                            "answer.title": answer_obj["title"],
                            "order": row_order,
                        },
                        {
                            "$set": {
                                "checklistRef": process_model["checklistRef"],
                                "inspectionDate": process_model["inspectionDate"],
                                "inspectionRef": process_model["inspectionRef"],
                                "userRef": userid,
                                "answer": answer_obj,
                                "order": row_order,
                                "commonId": q["commonId"],
                                "isBulkSystemPickList": "zeropicklist",
                            },
                        },
                        upsert=True
                    )
                )

        # Stream the CSV with logging
        stream_local_csv(
            local_csv_path,
            process_row,
            bulk_ops,
            MAX_BULK_OPS,
            checklist_inspection_collection
        )

        # Free memory
        bulk_ops.clear()
        common_id_map.clear()
        question_meta.clear()
        heap_usage()

        # Mark inspection completed
        payload = {
            "checklistRef": str(process_model["checklistRef"]),
            "inspectionRef": str(process_model["inspectionRef"]),
            "inspectionDate": (
                process_model["inspectionDate"].isoformat()
                if isinstance(process_model["inspectionDate"], datetime)
                else str(process_model["inspectionDate"])
            ),
            "score": 0,
            "order": 0,
            "userId": str(process_model["userinfo"]),
            "notifyQuestions": [],
            "statusColumn": "",
            "page": 1,
            "limit": 0,
            "sortBy": "",
            "sortOrder": "asc",
            "isQRupload": False,
            "isEvent": True  # Changed to True to bypass userRef filter in fetching, making data visible to all users
        }

        logger.info("Calling inspection_completed API...")
        result = inspection_completed(payload)

        if "error" in result:
            raise ValueError(
                f"Failed to complete inspection: {result['message']}")
        logger.info(f"✅ Inspection Completed: {result}")

        inspection_collection.update_one(
            {"_id": process_model["inspectionRef"]},
            {
                "$set": {
                    "inspectionType": "bulkExcelUpload",
                    "isBulkSystemPickList": "zeropicklistexcel",
                }
            }
        )
        checklist_file_upload_collection.update_one(
            {"_id": file_doc["_id"]},
            {"$set": {"status": "Completed"}}
        )
        logger.info(f"✅ File processing completed for {file_doc['_id']}")

    except Exception:
        logger.exception(f"❌ Error processing file: {file_doc['_id']}")
        checklist_file_upload_collection.update_one(
            {"_id": file_doc["_id"]},
            {"$set": {"status": "Failed", "errorMessage": "Processing error"}}
        )

# ---------------------------------------------------------------------------


def stream_local_csv(local_path, process_row, bulk_ops, max_bulk_ops, checklist_inspection_model):
    """Stream CSV rows and perform batched bulk writes with detailed logging."""
    headers = []
    is_header = True
    row_number = 0

    logger.info(f"Opening CSV file: {local_path}")
    try:
        # Changed to latin-1 to handle 0xa0 and similar bytes
        with open(local_path, "r", encoding="latin-1") as f:
            parser = csv.reader(
                f, delimiter="|", quoting=csv.QUOTE_NONE,
                skipinitialspace=True, strict=False
            )

            for row in parser:
                row = [v.strip() for v in row]  # Added full strip to match TS trim: true
                if is_header:
                    headers = row
                    is_header = False
                    logger.info(f"CSV headers detected: {headers}")
                    continue

                row_number += 1
                process_row(row, headers, bulk_ops)

                if len(bulk_ops) >= max_bulk_ops:
                    logger.info(
                        f"Flushing {len(bulk_ops)} ops at row {row_number}...")
                    checklist_inspection_model.bulk_write(
                        bulk_ops, ordered=False)
                    logger.info(
                        f"✅ DB updated with {len(bulk_ops)} records at row {row_number}")
                    print("Before clearing : ", heap_usage())
                    bulk_ops.clear()
                    print("After clearing : ", heap_usage())

                if row_number % 50 == 0:
                    time.sleep(0)  # optional pause

        if bulk_ops:
            logger.info(f"Flushing final {len(bulk_ops)} ops...")
            checklist_inspection_model.bulk_write(bulk_ops, ordered=False)
            logger.info(f"✅ Final DB update with {len(bulk_ops)} records")
            print("Before clearing : ", heap_usage())
            bulk_ops.clear()
            print("After clearing : ", heap_usage())

        logger.info(f"Finished reading {row_number} rows from CSV")
    except Exception:
        logger.exception("stream_local_csv failed")
        raise