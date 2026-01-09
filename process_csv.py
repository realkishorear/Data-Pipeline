import csv
import math
import os
import re
import sys
import resource
import multiprocessing
import tempfile
import shutil
from datetime import datetime
from bson import ObjectId
from dateutil import parser as date_parser
from dateutil.parser import parse
from pymongo import MongoClient, ReturnDocument
from helpers.apis import find_one, schedule_inspection_open
from helpers.dateTime_helper import check_date_and_time
from joblib import Parallel, delayed
from services.database.operations import create_checklistresult_if_not_exists

# Increase CSV field size limit to handle large fields
# Default limit is 131072 (128KB), set to maximum to avoid field size errors
csv.field_size_limit(sys.maxsize)

# Pre-compile regex patterns for performance
HTML_TAG_PATTERN = re.compile(r"<[^>]+>")

def heap_usage():
    """Log current process heap usage (Linux/macOS)."""
    usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024  # MB
    # Heap usage logging removed - using centralized logger

def normalize_header(text):
    """Optimized header normalization with pre-compiled regex."""
    text = str(text or "")
    text = HTML_TAG_PATTERN.sub("", text)
    return text.strip().lower()

def process_answer_value(value, question):
    """Process a single answer value and return the answer object."""
    answer_obj = {
        "_id": ObjectId(question["_id"]),
        "isHide": question.get("isHide", False),
        "type": question["type"],
        "title": question["title"],
        "checklistRef": ObjectId(question["checklistRef"]),
        "sectionRef": ObjectId(question["sectionRef"]),
        "createdAt": date_parser.isoparse(question["createdAt"]),
        "updatedAt": date_parser.isoparse(question["updatedAt"]),
        "__v": question.get("__v", 0),
        "checklistQuestionDetailsRef": question.get("checklistQuestionDetailsRef"),
        "checklistQuestionRef": question.get("checklistQuestionRef"),
        "indexOrder": question.get("indexOrder", 0),
        "details": question.get("details", []),
        "qtype": question.get("qtype"),
        "scorevalue": 0,
        "answer": None,
        "scoring": question.get("scoring"),
        "isDate": question.get("isDate", False),
        "isTime": question.get("isTime", False),
        "isSignature": question.get("isSignature", False),
        "mandatory": question.get("mandatory", False),
        "isAddnotes": question.get("isAddnotes", False),
        "ismultiselectdropdown": question.get("ismultiselectdropdown", False),
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
        # Error handling - errors are logged at higher level
        pass
    
    return answer_obj

def process_single_row_to_document(row, row_order, question_meta, checklist_ref, 
                                    inspection_ref, parsed_inspection_date, userid):
    """Process entire row and create ONE document containing all answers for that row."""
    now = datetime.utcnow()
    answers = []
    
    # Validate column count matches question count
    # Validation removed from logging - only errors are logged
    
    for i, q in enumerate(question_meta):
        if i < len(row):
            value = row[i].strip()
        else:
            value = None
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

    # Set CSV field size limit for worker process (separate process needs its own setting)
    csv.field_size_limit(sys.maxsize)

    client = MongoClient(os.getenv("DB_URL"))
    db = client.get_default_database()
    checklist_inspection_collection = db["checklistinspectionnews"]
    file_uploads_col = db["checklistfileuploads"]

    # Create a proper dialect instance with quote handling
    # Only use pipes or commas as delimiter
    if delimiter not in ('|', ','):
        delimiter = '|'
    
    # Use a factory function to create dialect to avoid scope issues
    # For pipe-delimited files, use QUOTE_NONE to avoid issues with malformed quotes
    def create_worker_dialect(delim):
        class CustomDialect(csv.excel):
            delimiter = delim
            skipinitialspace = True
            # For pipe-delimited files, disable quote handling to avoid malformed quote issues
            # For comma-delimited files, use minimal quoting
            if delim == '|':
                quoting = csv.QUOTE_NONE
                doublequote = False
            else:
                quoting = csv.QUOTE_MINIMAL
                doublequote = True
        return CustomDialect()
    
    documents_to_insert = []
    
    try:
        with open(chunk_path, "r", encoding="latin-1") as f:
            reader = csv.reader(f, dialect=create_worker_dialect(delimiter), strict=False)
            
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
                    checklist_inspection_collection.insert_many(documents_to_insert, ordered=False)
                    # Increment both the global lastRecord (cumulative across files)
                    # and the per-file processedRows (for resume/retry skipping).
                    file_uploads_col.update_one(
                        {"_id": file_id},
                        {"$inc": {"lastRecord": len_inserted, "processedRows": len_inserted}}
                    )
                    documents_to_insert.clear()
                    heap_usage()
            
            # Insert remaining documents
            if documents_to_insert:
                len_inserted = len(documents_to_insert)
                checklist_inspection_collection.insert_many(documents_to_insert, ordered=False)
                file_uploads_col.update_one(
                    {"_id": file_id},
                    {"$inc": {"lastRecord": len_inserted, "processedRows": len_inserted}}
                )
                documents_to_insert.clear()
                heap_usage()
        
        os.remove(chunk_path)
        return processed
        
    except Exception as e:
        from helpers.logger import logger
        import traceback
        error_details = traceback.format_exc()
        logger.error(f"process_chunk_optimized failed for chunk {chunk_path} (File ID: {file_id}): {str(e)}")
        logger.error(f"Error details for chunk {chunk_path}:\n{error_details}")
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
    
    # Ensure CSV field size limit is set (in case called from different context)
    csv.field_size_limit(sys.maxsize)
    
    try:
        # Detect delimiter - only consider pipes and commas
        with open(local_path, "r", encoding="latin-1") as f:
            sample = f.read(1024)
            
            # Count only pipes and commas (ignore other characters)
            comma_count = sample.count(',')
            pipe_count = sample.count('|')
            
            # Determine delimiter based on counts
            if pipe_count > comma_count:
                delimiter = '|'
            else:
                delimiter = ','
            
            # Create a proper dialect instance with quote handling
            # Use a factory function approach to avoid scope issues
            # For pipe-delimited files, use QUOTE_NONE to avoid issues with malformed quotes
            def create_dialect(delim):
                class CustomDialect(csv.excel):
                    delimiter = delim
                    skipinitialspace = True
                    # For pipe-delimited files, disable quote handling to avoid malformed quote issues
                    # For comma-delimited files, use minimal quoting
                    if delim == '|':
                        quoting = csv.QUOTE_NONE
                        doublequote = False
                    else:
                        quoting = csv.QUOTE_MINIMAL
                        doublequote = True
                return CustomDialect()
            
            dialect = create_dialect(delimiter)
            
            f.seek(0)
            csv_reader = csv.reader(f, dialect=dialect, strict=False)
            headers = next(csv_reader)
            
            # Validate delimiter choice - if we get too few columns, try the other delimiter
            if len(headers) < 3 and delimiter == ',' and pipe_count > 0:
                delimiter = '|'
                dialect = create_dialect('|')
                f.seek(0)
                csv_reader = csv.reader(f, dialect=dialect, strict=False)
                headers = next(csv_reader)
            elif len(headers) < 3 and delimiter == '|' and comma_count > 0:
                delimiter = ','
                dialect = create_dialect(',')
                f.seek(0)
                csv_reader = csv.reader(f, dialect=dialect, strict=False)
                headers = next(csv_reader)

        # Count total data rows
        total_data_rows = sum(1 for _ in open(local_path, "r", encoding="latin-1")) - 1

        remaining_rows = max(0, total_data_rows - skip_rows)
        if remaining_rows == 0:
            return

        # Optimize worker count
        num_workers = min(multiprocessing.cpu_count(), 16)
        if remaining_rows < num_workers * 100:
            num_workers = max(1, remaining_rows // 100)

        # Create temporary directory for chunks
        temp_dir = tempfile.mkdtemp(prefix='csv_chunks_')

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
                    ch_rows = chunk_size if i < num_workers - 1 else remaining_rows - current_offset
                    
                    # For QUOTE_NONE (pipe-delimited), write directly to avoid escape character issues
                    # For QUOTE_MINIMAL (comma-delimited), use csv.writer
                    if dialect.quoting == csv.QUOTE_NONE:
                        for _ in range(ch_rows):
                            try:
                                row = next(reader)
                                # Write directly as pipe-separated string
                                # Fields are already correctly parsed, so we just join them
                                # Replace newlines in fields to avoid breaking the row structure
                                cleaned_row = [str(field).replace('\n', ' ').replace('\r', '') for field in row]
                                line = dialect.delimiter.join(cleaned_row)
                                temp_f.write(line + '\n')
                            except StopIteration:
                                break
                    else:
                        # Use csv.writer for comma-delimited files with proper quote handling
                        writer = csv.writer(
                            temp_f,
                            delimiter=dialect.delimiter,
                            quoting=dialect.quoting,
                            doublequote=dialect.doublequote,
                            skipinitialspace=True
                        )
                        for _ in range(ch_rows):
                            try:
                                row = next(reader)
                                writer.writerow(row)
                            except StopIteration:
                                break
                
                chunk_paths.append(chunk_path)
                current_offset += ch_rows

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

        # Cleanup
        shutil.rmtree(temp_dir)

    except Exception as e:
        from helpers.logger import logger
        import traceback
        error_details = traceback.format_exc()
        logger.error(f"stream_local_csv_optimized failed for file {local_path} (File ID: {file_id}): {str(e)}")
        logger.error(f"Error details for CSV stream processing {local_path}:\n{error_details}")
        if 'temp_dir' in locals() and os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
        raise

def process_csv_file(local_csv_path: str):
    """Process a single CSV file and update Mongo collections accordingly."""

    # Verify file exists before processing
    if not os.path.exists(local_csv_path):
        from helpers.logger import logger
        logger.error(f"CSV file does not exist: {local_csv_path}")
        raise FileNotFoundError(f"CSV file does not exist: {local_csv_path}")

    client = MongoClient(os.getenv("DB_URL"))
    db = client.get_default_database()

    checklist_file_upload_collection = db["checklistfileuploads"]
    checklist_inspection_collection = db["checklistinspectionnews"]  # Changed collection name
    inspection_collection = db["inspections"]

    file_doc = checklist_file_upload_collection.find_one({"filePath": local_csv_path})
    if not file_doc:
        from helpers.logger import logger
        logger.warning(f"No file upload record found for path: {local_csv_path}")
        return

    try:
        updated_file = checklist_file_upload_collection.find_one_and_update(
            {"_id": file_doc["_id"], "status": {"$in": ["Pending", "Failed"]}},
            {"$set": {"status": "Processing"}},
            return_document=ReturnDocument.AFTER
        )

        if not updated_file:
            return

        process_model = updated_file

        # Ensure source
        if (src := process_model.get("source")) not in ("System", "UI"):
            checklist_file_upload_collection.update_one(
                {"_id": file_doc["_id"]}, {"$set": {"source": "UI"}}
            )

        # Load checklist questions
        check_data = find_one(process_model["checklistRef"])
        if "error" in check_data:
            raise ValueError(f"Failed to fetch checklist: {check_data['message']}")
        
        question_list = []
        for page in check_data.get("data", {}).get("page", []):
            for section in page.get("sections", []):
                for q in section.get("questions", []):
                    question_list.append(q)

        # Ensure inspectionRef
        if not process_model.get("inspectionRef"):
            inspection = schedule_inspection_open(process_model)
            if "error" in inspection:
                raise ValueError(f"Failed to schedule inspection: {inspection['message']}")
            process_model["inspectionRef"] = inspection["data"]["_id"]
            process_model["inspectionDate"] = inspection["data"]["inspectionDate"]

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

        # Free memory
        question_list.clear()
        heap_usage()

        # Determine starting order
        # - processedRows: how many rows of THIS file were already processed (for resume/retry)
        # - orderStartBase: global base order index at the start of this file
        processed_rows = process_model.get("processedRows", 0)
        order_start_base = process_model.get("orderStartBase")

        # Backwards compatibility: if orderStartBase is missing (old records),
        # treat lastRecord as both the global base and processed rows for this file.
        if order_start_base is None:
            order_start_base = max(0, int(process_model.get("lastRecord", 0) or 0) - int(processed_rows or 0))

        skip_rows = processed_rows
        start_order = order_start_base + processed_rows

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

        # Free memory
        common_id_map.clear()
        question_meta.clear()
        heap_usage()

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
        
        # Create checklistresult document if it doesn't exist
        create_checklistresult_if_not_exists(
            process_model["inspectionRef"],
            process_model.get("checklistRef"),
            process_model.get("source")
        )

    except Exception as e:
        from helpers.logger import logger
        import traceback
        error_details = traceback.format_exc()
        file_path = file_doc.get("filePath", local_csv_path) if 'file_doc' in locals() else local_csv_path
        file_id = file_doc.get("_id", "Unknown") if 'file_doc' in locals() else "Unknown"
        logger.error(f"Error processing CSV file: {file_path} (File ID: {file_id}): {str(e)}")
        logger.error(f"Error details for CSV file {file_path}:\n{error_details}")
        if 'checklist_file_upload_collection' in locals() and 'file_doc' in locals():
            checklist_file_upload_collection.update_one(
                {"_id": file_doc["_id"]},
                {"$set": {"status": "Failed", "errorMessage": f"Processing error: {str(e)}"}}
            )