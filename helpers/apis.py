import os
import logging
import requests
from requests.exceptions import RequestException, Timeout

# ✅ Configure logging
logging.basicConfig(
    level=logging.INFO,  # Change to DEBUG for more details
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def getAuthToken():
    # TODO: Implement token retrieval
    return os.getenv("JWT_TOKEN")  # Example placeholder

def find_one(checklist_id: str):
    """Fetch checklist details."""
    base_url = os.getenv("BASE_API_URL")
    jwt_token = getAuthToken().strip()
        
    if not base_url:
        logging.error("[find_one] BASE_API_URL not set")
        return {"error": "config", "message": "BASE_API_URL environment variable not set"}

    url = f"{base_url}/checklist/{checklist_id}"
    headers = {"Authorization": f"Bearer {jwt_token}"}
    payload = { "isExcel": True  }  # Payload for body

    logging.info(f"[find_one] Requesting checklist_id={checklist_id} from {url}")

    try:
        response = requests.get(url, headers=headers, json=payload, timeout=30)
        response.raise_for_status()  # Raise error for 4xx/5xx
        logging.info(f"[find_one] Success: {response.status_code}")
        return response.json()

    except Timeout:
        logging.error("[find_one] Request timed out")
        return {"error": "timeout", "message": "The request timed out"}
    except RequestException as e:
        logging.exception("[find_one] Request failed")
        return {"error": "request_failed", "message": str(e)}
    except Exception as e:
        logging.exception("[find_one] Unexpected error")
        return {"error": "unexpected", "message": str(e)}

def schedule_inspection_open(process_model: dict):
    """Open a scheduled inspection."""
    base_url = os.getenv("BASE_API_URL")
    jwt_token = getAuthToken().strip()
    if not base_url:
        logging.error("[schedule_inspection_open] BASE_API_URL not set")
        return {"error": "config", "message": "BASE_API_URL environment variable not set"}

    url = f"{base_url}/checklist/scheduleopen"
    payload = {"checklistRef": process_model.get("checklistRef")}
    headers = {
        "Authorization": f"Bearer {jwt_token}",
        "Content-Type": "application/json",
        "company": process_model.get("companyRef", ""),
        "facility": process_model.get("facilityRef", "")
    }

    logging.info(f"[schedule_inspection_open] Sending request to {url}")

    try:
        response = requests.post(url, json=payload, headers=headers, timeout=30)
        response.raise_for_status()
        logging.info(f"[schedule_inspection_open] Success: {response.status_code}")
        return response.json()

    except Timeout:
        logging.error("[schedule_inspection_open] Request timed out")
        return {"error": "timeout", "message": "The request timed out"}
    except RequestException as e:
        logging.exception("[schedule_inspection_open] Request failed")
        return {"error": "request_failed", "message": str(e)}
    except Exception as e:
        logging.exception("[schedule_inspection_open] Unexpected error")
        return {"error": "unexpected", "message": str(e)}

def inspection_completed(body: dict):
    """Mark an inspection as completed."""
    base_url = os.getenv("BASE_API_URL")
    jwt_token = getAuthToken().strip()
    if not base_url:
        logging.error("[inspection_completed] BASE_API_URL not set")
        return {"error": "config", "message": "BASE_API_URL environment variable not set"}

    url = f"{base_url}/checklist/inspectioncompleted"
    headers = {
        "Authorization": f"Bearer {jwt_token}",
        "Content-Type": "application/json"
    }

    logging.info("[inspection_completed] Sending inspection completion data")

    try:
        response = requests.post(url, json=body, headers=headers, timeout=30)
        response.raise_for_status()
        logging.info(f"[inspection_completed] Success: {response.status_code}")
        return response.json()

    except Timeout:
        logging.error("[inspection_completed] Request timed out")
        return {"error": "timeout", "message": "The request timed out"}
    except RequestException as e:
        logging.exception("[inspection_completed] Request failed")
        return {"error": "request_failed", "message": str(e)}
    except Exception as e:
        logging.exception("[inspection_completed] Unexpected error")
        return {"error": "unexpected", "message": str(e)}