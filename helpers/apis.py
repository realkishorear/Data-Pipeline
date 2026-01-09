import os
import requests
from requests.exceptions import RequestException, Timeout
from helpers.logger import logger

def getAuthToken():
    
    base_url = os.getenv("BASE_API_URL")
    url = f"{base_url}/auth/login"
    payload = {
        "email" : "sid1@knowella.com",
        "password" : "Testing@1234321",
        "withoutToken" : True
    }
    headers = {
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.post(url, json=payload, headers=headers, timeout=30)
        response.raise_for_status()
        authTokenJSON = response.json()
        auth_token = authTokenJSON['data']['access_token']
        return auth_token

    except Timeout:
        logger.error("[getAuthToken] Request timed out")
        return {"error": "timeout", "message": "The request timed out"}
    except RequestException as e:
        logger.error(f"[getAuthToken] Request failed: {e}")
        return {"error": "request_failed", "message": str(e)}
    except Exception as e:
        logger.error(f"[getAuthToken] Unexpected error: {e}")
        return {"error": "unexpected", "message": str(e)}
    
    
    return os.getenv("JWT_TOKEN")  # Example placeholder

def find_one(checklist_id: str):
    """Fetch checklist details."""
    base_url = os.getenv("BASE_API_URL")
    jwt_token = getAuthToken().strip()
        
    if not base_url:
        logger.error("[find_one] BASE_API_URL not set")
        return {"error": "config", "message": "BASE_API_URL environment variable not set"}

    url = f"{base_url}/checklist/{checklist_id}"
    headers = {"Authorization": f"Bearer {jwt_token}"}
    payload = { "isExcel": True  }  # Payload for body

    try:
        response = requests.get(url, headers=headers, json=payload, timeout=30)
        response.raise_for_status()  # Raise error for 4xx/5xx
        return response.json()

    except Timeout:
        logger.error("[find_one] Request timed out")
        return {"error": "timeout", "message": "The request timed out"}
    except RequestException as e:
        logger.error(f"[find_one] Request failed: {e}")
        return {"error": "request_failed", "message": str(e)}
    except Exception as e:
        logger.error(f"[find_one] Unexpected error: {e}")
        return {"error": "unexpected", "message": str(e)}

def schedule_inspection_open(process_model: dict):
    """Open a scheduled inspection."""
    base_url = os.getenv("BASE_API_URL")
    jwt_token = getAuthToken().strip()
    if not base_url:
        logger.error("[schedule_inspection_open] BASE_API_URL not set")
        return {"error": "config", "message": "BASE_API_URL environment variable not set"}

    url = f"{base_url}/checklist/scheduleopen"
    payload = {"checklistRef": process_model.get("checklistRef")}
    headers = {
        "Authorization": f"Bearer {jwt_token}",
        "Content-Type": "application/json",
        "company": process_model.get("companyRef", ""),
        "facility": process_model.get("facilityRef", "")
    }

    try:
        response = requests.post(url, json=payload, headers=headers, timeout=30)
        response.raise_for_status()
        return response.json()

    except Timeout:
        logger.error("[schedule_inspection_open] Request timed out")
        return {"error": "timeout", "message": "The request timed out"}
    except RequestException as e:
        logger.error(f"[schedule_inspection_open] Request failed: {e}")
        return {"error": "request_failed", "message": str(e)}
    except Exception as e:
        logger.error(f"[schedule_inspection_open] Unexpected error: {e}")
        return {"error": "unexpected", "message": str(e)}

def inspection_completed(body: dict):
    """Mark an inspection as completed."""
    base_url = os.getenv("BASE_API_URL")
    jwt_token = getAuthToken().strip()
    if not base_url:
        logger.error("[inspection_completed] BASE_API_URL not set")
        return {"error": "config", "message": "BASE_API_URL environment variable not set"}

    url = f"{base_url}/checklist/inspectioncompleted"
    headers = {
        "Authorization": f"Bearer {jwt_token}",
        "Content-Type": "application/json"
    }

    try:
        response = requests.post(url, json=body, headers=headers, timeout=30)
        response.raise_for_status()
        return response.json()

    except Timeout:
        logger.error("[inspection_completed] Request timed out")
        return {"error": "timeout", "message": "The request timed out"}
    except RequestException as e:
        logger.error(f"[inspection_completed] Request failed: {e}")
        return {"error": "request_failed", "message": str(e)}
    except Exception as e:
        logger.error(f"[inspection_completed] Unexpected error: {e}")
        return {"error": "unexpected", "message": str(e)}