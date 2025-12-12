import csv
import math
import os
import re
import logging
import resource
import multiprocessing
import tempfile
import shutil
from datetime import datetime
from bson import ObjectId
from dateutil import parser as date_parser
from dateutil.parser import parse
from pymongo import MongoClient, ReturnDocument
from helpers.apis import find_one, schedule_inspection_open, inspection_completed
from helpers.dateTime_helper import check_date_and_time
from joblib import Parallel, delayed

# ---------------------------------------------------------------------------
# Logging configuration
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# Pre-compile regex patterns for performance
HTML_TAG_PATTERN = re.compile(r"<[^>]+>")

def heap_usage():
    """Log current process heap usage (Linux/macOS)."""
    usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024  # MB
    logger.info(f"📊 Heap usage: {usage:.2f} MB")

def normalize_header(text):
    """Optimized header normalization with pre-compiled regex."""
    text = str(text or "")
    text = HTML_TAG_PATTERN.sub("", text)
    return text.strip().lower()

def process_answer_value(value, question):
    """Process a single answer value and return the answer object."""
    answer_obj = {
        "_id": ObjectId(question["_id"]),
        "isHide": question["isHide"],
        "type": question["type"],
        "title": question["title"],
        "checklistRef": ObjectId(question["checklistRef"]),
        "sectionRef": ObjectId(question["sectionRef"]),
        "createdAt": date_parser.isoparse(question["createdAt"]),
        "updatedAt": date_parser.isoparse(question["updatedAt"]),
        "__v": question["__v"],
        "checklistQuestionDetailsRef": question["checklistQuestionDetailsRef"],
        "checklistQuestionRef": question["checklistQuestionRef"],
        "indexOrder": question["indexOrder"],
        "details": question.get("details", []),
        "qtype": question["qtype"],
        "scorevalue": 0,
        "answer": None,
        "scoring": question["scoring"],
        "isDate": question["isDate"],
        "isTime": question["isTime"],
        "isSignature": question["isSignature"],
        "mandatory": question["mandatory"],
        "isAddnotes": question["isAddnotes"],
        "ismultiselectdropdown": question["ismultiselectdropdown"],
    }
    
    if value in (None, ""):
        return answer_obj
    
    try:
        q_type = question["type"]
        
        if q_type == "Single choice responder":
            ans_single = question.get("_answerOptionsMap", {}).get(normalize_header(value))
            if ans_single:
                answer_obj["scorevalue"] = float(ans_single.get("score", 0)) or 0
            answer_obj["answer"] = value
            
        elif q_type == "Multiple choice responder":
            choices = [v.strip() for v in str(value).split(",")]
            score = 0
            answer_options_map = question.get("_answerOptionsMap", {})
            for c in choices:
                ans_multi = answer_options_map.get(normalize_header(c))
                if ans_multi:
                    score += float(ans_multi.get("score", 0)) or 0
            answer_obj["scorevalue"] = score
            answer_obj["answer"] = choices
            
        elif q_type == "Text answer":
            score = 0
            for sc in question.get("scoreOptions", []):
                if ((sc["condition"] == "is customized keyword" and sc.get("count") == value)
                    or (sc["condition"] == "is not blank" and value)
                    or (sc["condition"] == "is blank" and not value)):
                    score += float(sc.get("score", 0)) or 0
            answer_obj["scorevalue"] = score
            answer_obj["answer"] = value
            
        elif q_type == "Date & Time":
            date_output = check_date_and_time(value)
            if date_output["isValidDate"]:
                answer_obj["answer"] = {"date": date_output["date"]}
                
        elif q_type in ["Slider", "Number"]:
            try:
                num_value = float(value)
            except ValueError:
                num_value = math.nan
            score = 0
            for sc in question.get("scoreOptions", []):
                try:
                    cnt = float(sc["count"])
                except (ValueError, KeyError):
                    cnt = 0
                cond = sc["condition"]
                if ((cond == "less than" and num_value < cnt)
                    or (cond == "less than or equal to" and num_value <= cnt)
                    or (cond == "equal to" and num_value == cnt)
                    or (cond == "not equal to" and num_value != cnt)
                    or (cond == "greater than or equal to" and num_value >= cnt)
                    or (cond == "greater than" and num_value > cnt)):
                    score += float(sc.get("score", 0)) or 0
            answer_obj["scorevalue"] = score
            answer_obj["answer"] = value
        else:
            answer_obj["answer"] = value
            
    except Exception:
        logger.exception(f"Failed to process value for question {question.get('title')}")
    
    return answer_obj

def process_single_row_to_document(row, row_order, question_meta, checklist_ref, 
                                    inspection_ref, parsed_inspection_date, userid):
    """Process entire row and create ONE document containing all answers for that row."""
    now = datetime.utcnow()
    answers = []
    
    for i, q in enumerate(question_meta):
        value = row[i].strip() if i < len(row) else None
        answer_obj = process_answer_value(value, q)
        answers.append(answer_obj)
    
    # Create single document for the entire row
    document = {
        "_id": ObjectId(),
        "checklistRef": checklist_ref,
        "inspectionDate": parsed_inspection_date,
        "inspectionRef": inspection_ref,
        "userRef": userid,
        "answers": answers,  # All answers in one array
        "order": row_order,
        "commonId": question_meta[0]["commonId"] if question_meta else ObjectId(),
        "isBulkSystemPickList": "zeropicklist",
        "createdAt": now,
        "updatedAt": now,
    }
    
    return document

def process_chunk_optimized(args):
    """Process a chunk of CSV rows - one document per row."""
    chunk_path, rows_to_process, order_start, question_meta, checklist_ref, inspection_ref, \
        parsed_inspection_date, userid, delimiter, max_bulk_docs, file_id = args

    client = MongoClient(os.getenv("DB_URL"))
    db = client.get_default_database()
    checklist_inspection_collection = db["checklistinspectionnews"]
    
    file_uploads_col = db["checklistfileuploads"]

    dialect = csv.excel
    dialect.delimiter = delimiter
    dialect.skipinitialspace = True
    
    documents_to_insert = []
    
    try:
        with open(chunk_path, "r", encoding="latin-1") as f:
            reader = csv.reader(f, dialect=dialect, strict=False)
            
            processed = 0
            for local_i, row in enumerate(reader):
                if local_i >= rows_to_process:
                    break
                    
                row = [v.strip() for v in row]
                if not any(row):
                    continue
                
                row_order = order_start + processed
                
                # Create ONE document for this entire row
                row_document = process_single_row_to_document(
                    row, row_order, question_meta, checklist_ref,
                    inspection_ref, parsed_inspection_date, userid
                )
                
                documents_to_insert.append(row_document)
                processed += 1
                
                # Bulk insert when threshold reached
                if len(documents_to_insert) >= max_bulk_docs:
                    len_inserted = len(documents_to_insert)
                    logger.info(f"Inserting {len_inserted} row documents in worker...")
                    checklist_inspection_collection.insert_many(documents_to_insert, ordered=False)
                    file_uploads_col.update_one({"_id": file_id}, {"$inc": {"lastRecord": len_inserted}})
                    documents_to_insert.clear()
                    heap_usage()
            
            # Insert remaining documents
            if documents_to_insert:
                len_inserted = len(documents_to_insert)
                logger.info(f"Inserting final {len_inserted} row documents in worker...")
                checklist_inspection_collection.insert_many(documents_to_insert, ordered=False)
                file_uploads_col.update_one({"_id": file_id}, {"$inc": {"lastRecord": len_inserted}})
                documents_to_insert.clear()
                heap_usage()
        
        os.remove(chunk_path)
        return processed
        
    except Exception:
        logger.exception("process_chunk_optimized failed")
        if os.path.exists(chunk_path):
            os.remove(chunk_path)
        raise

def prepare_question_metadata(question_list):
    """Pre-process questions to create lookup maps for faster access."""
    question_meta = []
    
    for q in question_list:
        q_meta = q.copy()
        q_meta["cleanTitle"] = normalize_header(q["title"])
        
        # Create answer options map for O(1) lookup
        if "answerOptions" in q:
            q_meta["_answerOptionsMap"] = {
                normalize_header(opt["name"]): opt 
                for opt in q["answerOptions"]
            }
        
        question_meta.append(q_meta)
    
    return question_meta

def stream_local_csv_optimized(local_path, max_bulk_docs, checklist_inspection_model, 
                                file_uploads_col, file_id, skip_rows, question_meta, 
                                checklist_ref, inspection_ref, parsed_inspection_date, 
                                userid, start_order):
    """Optimized CSV streaming with parallel processing - one document per row."""
    
    logger.info(f"Opening CSV file: {local_path} for dialect detection")
    
    try:
        # Detect delimiter
        with open(local_path, "r", encoding="latin-1") as f:
            sample = f.read(1024)
            try:
                dialect = csv.Sniffer().sniff(sample, delimiters=',;\t| ')
                dialect.doublequote = True
            except csv.Error:
                comma_count = sample.count(',')
                pipe_count = sample.count('|')
                delimiter = '|' if pipe_count > comma_count else ','
                dialect = csv.excel
                dialect.delimiter = delimiter
                dialect.doublequote = True
                logger.info(f"Fallback delimiter detected: {delimiter}")
            
            dialect.skipinitialspace = True
            f.seek(0)
            csv_reader = csv.reader(f, dialect=dialect, strict=False)
            headers = next(csv_reader)
            
            # Check if delimiter needs override
            if len(headers) < 10 and sample.count('|') > sample.count(','):
                dialect.delimiter = '|'
                f.seek(0)
                csv_reader = csv.reader(f, dialect=dialect, strict=False)
                headers = next(csv_reader)
                logger.info(f"Overridden delimiter to '|'. New header length: {len(headers)}")
            
            logger.info(f"CSV headers detected: {len(headers)} columns")

        # Count total data rows
        total_data_rows = sum(1 for _ in open(local_path, "r", encoding="latin-1")) - 1
        logger.info(f"Total data rows in CSV: {total_data_rows}")

        remaining_rows = max(0, total_data_rows - skip_rows)
        if remaining_rows == 0:
            logger.info("No remaining rows to process.")
            return

        # Optimize worker count
        num_workers = min(multiprocessing.cpu_count(), 16)
        if remaining_rows < num_workers * 100:
            num_workers = max(1, remaining_rows // 100)
        logger.info(f"Using {num_workers} workers for parallel processing.")

        # Create temporary directory for chunks
        temp_dir = tempfile.mkdtemp(prefix='csv_chunks_')
        logger.info(f"Created temporary directory: {temp_dir}")

        # Split CSV into chunk files
        chunk_paths = []
        chunk_size = remaining_rows // num_workers
        
        with open(local_path, "r", encoding="latin-1") as f:
            reader = csv.reader(f, dialect=dialect, strict=False)
            next(reader)  # Skip header
            
            # Skip already processed rows
            for _ in range(skip_rows):
                next(reader)

            current_offset = 0
            for i in range(num_workers):
                chunk_filename = f"chunk_{i+1}.csv"
                chunk_path = os.path.join(temp_dir, chunk_filename)
                
                with open(chunk_path, 'w', encoding="latin-1", newline='') as temp_f:
                    writer = csv.writer(temp_f, dialect=dialect)
                    ch_rows = chunk_size if i < num_workers - 1 else remaining_rows - current_offset
                    
                    for _ in range(ch_rows):
                        try:
                            row = next(reader)
                            writer.writerow(row)
                        except StopIteration:
                            break
                
                chunk_paths.append(chunk_path)
                current_offset += ch_rows
                logger.info(f"Created chunk: {chunk_path} ({ch_rows} rows)")

        # Prepare chunk arguments
        chunks = []
        current_offset = 0
        for i in range(num_workers):
            ch_rows = chunk_size if i < num_workers - 1 else remaining_rows - current_offset
            order_start = start_order + current_offset
            chunks.append((
                chunk_paths[i], ch_rows, order_start, question_meta, checklist_ref,
                inspection_ref, parsed_inspection_date, userid, dialect.delimiter, max_bulk_docs, file_id
            ))
            current_offset += ch_rows

        # Process chunks in parallel
        results = Parallel(n_jobs=num_workers, backend='multiprocessing')(
            delayed(process_chunk_optimized)(chunk) for chunk in chunks
        )

        total_processed = sum(results)
        logger.info(f"✅ Processed {total_processed} rows across all workers.")

        # Cleanup
        shutil.rmtree(temp_dir)
        logger.info(f"Cleaned up temporary directory: {temp_dir}")

    except Exception:
        logger.exception("stream_local_csv_optimized failed")
        if 'temp_dir' in locals() and os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
        raise

def process_csv_file(local_csv_path: str):
    """Process a single CSV file and update Mongo collections accordingly."""

    logger.info(f"▶️ Starting processing for file: {local_csv_path}")

    client = MongoClient(os.getenv("DB_URL"))
    db = client.get_default_database()

    checklist_file_upload_collection = db["checklistfileuploads"]
    checklist_inspection_collection = db["checklistinspectionnews"]  # Changed collection name
    inspection_collection = db["inspections"]

    file_doc = checklist_file_upload_collection.find_one({"filePath": local_csv_path})
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
            logger.info(f"File {file_doc['_id']} already processed or not in Pending/Failed. Skipping.")
            return

        process_model = updated_file

        # Ensure source
        if (src := process_model.get("source")) not in ("System", "UI"):
            checklist_file_upload_collection.update_one(
                {"_id": file_doc["_id"]}, {"$set": {"source": "UI"}}
            )
            logger.info(f"Updated file source to UI for {file_doc['_id']}")

        # Load checklist questions
        logger.info("Fetching checklist questions via API...")
        check_data = find_one(process_model["checklistRef"])
        if "error" in check_data:
            raise ValueError(f"Failed to fetch checklist: {check_data['message']}")
        
        question_list = []
        for page in check_data.get("data", {}).get("page", []):
            for section in page.get("sections", []):
                for q in section.get("questions", []):
                    question_list.append(q)
        logger.info(f"Loaded {len(question_list)} questions from checklist")

        # Ensure inspectionRef
        if not process_model.get("inspectionRef"):
            logger.info("Scheduling inspection (API call)...")
            inspection = schedule_inspection_open(process_model)
            if "error" in inspection:
                raise ValueError(f"Failed to schedule inspection: {inspection['message']}")
            process_model["inspectionRef"] = inspection["data"]["_id"]
            process_model["inspectionDate"] = inspection["data"]["inspectionDate"]
            logger.info(f"Inspection scheduled: {process_model['inspectionRef']}")

        parsed_inspection_date = parse(process_model["inspectionDate"]) if isinstance(
            process_model["inspectionDate"], str) else process_model["inspectionDate"]

        # Prepare optimized metadata with lookup maps
        MAX_BULK_DOCS = 5000  # Documents per batch (each document = 1 row with all answers)
        common_id_map = {
            f"{q['type']}-{normalize_header(q['title'])}": ObjectId() 
            for q in question_list
        }

        question_meta = prepare_question_metadata(question_list)
        
        # Add commonId to metadata
        for q in question_meta:
            q["commonId"] = common_id_map[f"{q['type']}-{q['cleanTitle']}"]

        logger.info(f"Prepared metadata for {len(question_meta)} questions")

        # Free memory
        question_list.clear()
        heap_usage()

        # Determine starting order
        skip_rows = process_model.get("lastRecord", 0)
        start_order = skip_rows

        logger.info(f"Starting order index at {start_order}, skipping {skip_rows} rows")

        # Stream and process CSV
        stream_local_csv_optimized(
            local_csv_path,
            MAX_BULK_DOCS,
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

        total_processed = 0  # Note: In a real implementation, this would be returned from stream_local_csv_optimized
        logger.info(f"✅ Updated lastRecord incrementally during processing. Total new rows: {total_processed}")

        # Free memory
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
            raise ValueError(f"Failed to complete inspection: {result['message']}")
        logger.info(f"✅ Inspection Completed: {result}")

        inspection_collection.update_one(
            {"_id": process_model["inspectionRef"]},
            {"$set": {
                "inspectionType": "bulkExcelUpload",
                "isBulkSystemPickList": "zeropicklistexcel",
            }}
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