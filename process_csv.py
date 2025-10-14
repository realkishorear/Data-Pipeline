import csv
import math
import os
import re
import time
import logging
import resource
import multiprocessing
import tempfile
import shutil
from datetime import datetime
from bson import ObjectId
from dateutil import parser as date_parser
from dateutil.parser import parse
from pymongo import MongoClient, UpdateOne, ReturnDocument
from helpers.apis import find_one, schedule_inspection_open, inspection_completed
from helpers.dateTime_helper import check_date_and_time
from joblib import Parallel, delayed

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


def normalize_header(text):
    text = str(text or "")
    text = re.sub(r"<[^>]+>", "", text)  # remove HTML
    return text.strip().lower()


def process_single_row(row, row_order, question_meta, checklist_ref, inspection_ref, parsed_inspection_date, userid):
    ops = []
    for i, q in enumerate(question_meta):
        value = row[i].strip() if i < len(row) else None
        answer_obj = {
            "_id": ObjectId(q["_id"]),
            "isHide": q["isHide"],
            "type": q["type"],
            "title": q["title"],
            "checklistRef": ObjectId(q["checklistRef"]),
            "sectionRef": ObjectId(q["sectionRef"]),
            "createdAt": date_parser.isoparse(q["createdAt"]),
            "updatedAt": date_parser.isoparse(q["updatedAt"]),
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

        ops.append(
            UpdateOne(
                filter={
                    "inspectionRef": inspection_ref,
                    "checklistRef": checklist_ref,
                    "answer.title": answer_obj["title"],
                    "order": row_order,
                },
                update={
                    "$set": {
                        "checklistRef": checklist_ref,
                        "inspectionDate": parsed_inspection_date,
                        "inspectionRef": inspection_ref,
                        "userRef": userid,
                        "answer": answer_obj,
                        "order": row_order,
                        "commonId": q["commonId"],
                        "isBulkSystemPickList": "zeropicklist",
                        "updatedAt": datetime.utcnow(),
                    },
                    "$setOnInsert": {
                        "createdAt": datetime.utcnow(),
                    },
                },
                upsert=True
            )
        )
    return ops


def process_csv_file(local_csv_path: str):
    """
    Process a single CSV file and update Mongo collections accordingly.
    """

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
            {"_id": file_doc["_id"], "status": {"$in": ["Pending", "Failed"]}},
            {"$set": {"status": "Processing"}},
            return_document=ReturnDocument.AFTER
        )

        if not updated_file:
            logger.info(
                f"File {file_doc['_id']} already processed or not in Pending/Failed. Skipping.")
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

        parsed_inspection_date = parse(process_model["inspectionDate"]) if isinstance(
            process_model["inspectionDate"], str) else process_model["inspectionDate"]

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

        # Adjust skip_rows to the last complete row
        skip_rows = process_model.get("lastRecord", 0)
        Q = len(question_meta)
        if Q > 0:
            current_skip = skip_rows
            while current_skip > 0:
                order_to_check = start_order + current_skip - 1
                count = checklist_inspection_collection.count_documents({
                    "inspectionRef": process_model["inspectionRef"],
                    "checklistRef": process_model["checklistRef"],
                    "order": order_to_check
                })
                if count == Q:
                    break
                else:
                    current_skip -= 1
            if current_skip != skip_rows:
                logger.info(
                    f"Adjusted skip_rows from {skip_rows} to {current_skip} based on complete rows in DB")
                checklist_file_upload_collection.update_one(
                    {"_id": file_doc["_id"]},
                    {"$set": {"lastRecord": current_skip}}
                )
                skip_rows = current_skip

        # Stream the CSV with logging
        stream_local_csv(
            local_csv_path,
            bulk_ops,
            MAX_BULK_OPS,
            checklist_inspection_collection,
            checklist_file_upload_collection,
            file_doc["_id"],
            skip_rows,
            question_meta,
            process_model["checklistRef"],
            process_model["inspectionRef"],
            parsed_inspection_date,
            process_model["userinfo"],
            start_order
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
            "isEvent": False,
            "publicloginFirstName": "",
            "publicloginLastName": "",
            "publicloginEmail": ""
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


def process_chunk_copy(args):
    chunk_path, rows_to_process, order_start, question_meta, checklist_ref, inspection_ref, parsed_inspection_date, userid, delimiter, max_bulk_ops = args

    client = MongoClient(os.getenv("DB_URL"))
    db = client.get_default_database()
    checklist_inspection_collection = db["checklistinspections"]

    bulk_ops = []
    dialect = csv.excel
    dialect.delimiter = delimiter
    dialect.skipinitialspace = True

    try:
        with open(chunk_path, "r", encoding="latin-1") as f:
            reader = csv.reader(f, dialect=dialect, strict=False)

            processed = 0
            batch = []
            batch_size = 500 
            local_i = 0
            while local_i < rows_to_process:
                try:
                    row = next(reader)
                except StopIteration:
                    break
                row = [v.strip() for v in row]
                local_i += 1
                if not any(row):
                    continue

                batch.append(row)

                if len(batch) >= batch_size:
                    row_orders = [order_start + processed +
                                  j for j in range(len(batch))]
                    results = Parallel(n_jobs=-1, backend='threading')(
                        delayed(process_single_row)(
                            batch[j],
                            row_orders[j],
                            question_meta,
                            checklist_ref,
                            inspection_ref,
                            parsed_inspection_date,
                            userid
                        ) for j in range(len(batch))
                    )

                    for ops in results:
                        bulk_ops.extend(ops)
                        if len(bulk_ops) >= max_bulk_ops:
                            logger.info(
                                f"Flushing {len(bulk_ops)} ops in worker...")
                            checklist_inspection_collection.bulk_write(
                                bulk_ops, ordered=False)
                            bulk_ops.clear()
                            heap_usage()

                    processed += len(batch)
                    batch = []

            # Process final sub-batch
            if batch:
                row_orders = [order_start + processed +
                              j for j in range(len(batch))]
                results = Parallel(n_jobs=-1, backend='threading')(
                    delayed(process_single_row)(
                        batch[j],
                        row_orders[j],
                        question_meta,
                        checklist_ref,
                        inspection_ref,
                        parsed_inspection_date,
                        userid
                    ) for j in range(len(batch))
                )

                for ops in results:
                    bulk_ops.extend(ops)
                    if len(bulk_ops) >= max_bulk_ops:
                        logger.info(
                            f"Flushing {len(bulk_ops)} ops in worker...")
                        checklist_inspection_collection.bulk_write(
                            bulk_ops, ordered=False)
                        bulk_ops.clear()
                        heap_usage()

                processed += len(batch)

            if bulk_ops:
                logger.info(f"Flushing final {len(bulk_ops)} ops in worker...")
                checklist_inspection_collection.bulk_write(
                    bulk_ops, ordered=False)
                bulk_ops.clear()
                heap_usage()

        os.remove(chunk_path)  # Clean up temp chunk file
        return processed
    except Exception:
        logger.exception("process_chunk_copy failed")
        if os.path.exists(chunk_path):
            os.remove(chunk_path)
        raise


def stream_local_csv(local_path, bulk_ops, max_bulk_ops, checklist_inspection_model, file_uploads_col, file_id, skip_rows, question_meta, checklist_ref, inspection_ref, parsed_inspection_date, userid, start_order):
    """Stream CSV rows and perform batched bulk writes with detailed logging and parallel chunk processing using temp chunk copies."""
    headers = []
    logger.info(f"Opening CSV file: {local_path} for dialect detection")
    try:
        with open(local_path, "r", encoding="latin-1") as f:
            sample = f.read(1024)
            try:
                dialect = csv.Sniffer().sniff(sample, delimiters=',;\t| ')
                dialect.doublequote = True
            except csv.Error:
                # Fallback to counting delimiters
                comma_count = sample.count(',')
                pipe_count = sample.count('|')
                if pipe_count > comma_count:
                    delimiter = '|'
                else:
                    delimiter = ','
                dialect = csv.excel
                dialect.delimiter = delimiter
                dialect.doublequote = True
                logger.info(f"Fallback delimiter detected: {delimiter}")
            dialect.skipinitialspace = True
            f.seek(0)
            csv_reader = csv.reader(
                f, dialect=dialect, strict=False
            )
            headers = next(csv_reader)
            # Check if delimiter needs override
            sample_comma = sample.count(',')
            sample_pipe = sample.count('|')
            if len(headers) < 10 and sample_pipe > sample_comma:
                dialect.delimiter = '|'
                dialect.doublequote = True
                f.seek(0)
                csv_reader = csv.reader(
                    f, dialect=dialect, strict=False
                )
                headers = next(csv_reader)
                logger.info(
                    f"Overridden delimiter to '|' based on count comparison. New header length: {len(headers)}")
            logger.info(f"CSV headers detected: {headers}")

        # Count total data rows (excluding header)
        total_data_rows = 0
        with open(local_path, "r", encoding="latin-1") as f:
            reader = csv.reader(f, dialect=dialect, strict=False)
            next(reader)  # skip header
            for row in reader:
                total_data_rows += 1
        logger.info(f"Total data rows in CSV: {total_data_rows}")

        remaining_rows = max(0, total_data_rows - skip_rows)
        if remaining_rows == 0:
            logger.info("No remaining rows to process.")
            return

        # Optimal number of workers
        num_workers = min(multiprocessing.cpu_count() // 2, 8)
        if remaining_rows < num_workers * 10:
            num_workers = max(1, remaining_rows // 10)
        logger.info(f"Using {num_workers} workers for parallel processing.")

        # Create a temporary directory for chunks
        temp_dir = tempfile.mkdtemp(prefix='csv_chunks_')
        logger.info(f"Created temporary directory for chunks: {temp_dir}")

        # Create chunk files with unique names like file1.csv, file2.csv, etc.
        chunk_paths = []
        chunk_size = remaining_rows // num_workers
        with open(local_path, "r", encoding="latin-1") as f:
            reader = csv.reader(f, dialect=dialect, strict=False)
            next(reader)  # Skip header
            for _ in range(skip_rows):
                next(reader)  # Skip processed rows

            current_offset = 0
            for i in range(num_workers):
                # Unique file name
                chunk_filename = f"file{i+1}.csv"
                chunk_path = os.path.join(temp_dir, chunk_filename)

                with open(chunk_path, 'w', encoding="latin-1", newline='') as temp_f:
                    writer = csv.writer(temp_f, dialect=dialect)
                    ch_rows = chunk_size if i < num_workers - \
                        1 else remaining_rows - current_offset
                    for _ in range(ch_rows):
                        try:
                            row = next(reader)
                            writer.writerow(row)
                        except StopIteration:
                            break
                chunk_paths.append(chunk_path)
                current_offset += ch_rows
                logger.info(
                    f"Created temp chunk file: {chunk_path} with {ch_rows} rows")

        chunks = []
        current_offset = 0
        for i in range(num_workers):
            ch_rows = chunk_size if i < num_workers - \
                1 else remaining_rows - current_offset
            order_start = start_order + current_offset
            chunks.append((
                chunk_paths[i], ch_rows, order_start, question_meta, checklist_ref, inspection_ref,
                parsed_inspection_date, userid, dialect.delimiter, max_bulk_ops
            ))
            current_offset += ch_rows

        # Use joblib Parallel with multiprocessing backend for chunk processing
        results = Parallel(n_jobs=num_workers, backend='multiprocessing')(
            delayed(process_chunk_copy)(chunk) for chunk in chunks
        )

        total_processed = sum(results)
        logger.info(f"Processed {total_processed} rows across all workers.")

        # Update lastRecord at the end
        new_last = skip_rows + total_data_rows
        file_uploads_col.update_one(
            {"_id": file_id},
            {"$set": {"lastRecord": new_last}}
        )
        logger.info(f"✅ Updated lastRecord to {new_last}")

        # Clean up the temporary directory
        shutil.rmtree(temp_dir)
        logger.info(f"Cleaned up temporary directory: {temp_dir}")

    except Exception:
        logger.exception("stream_local_csv failed")
        # Clean up any remaining temp files and directory
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
        raise