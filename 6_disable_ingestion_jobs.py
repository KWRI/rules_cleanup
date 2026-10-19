"""
Pipeline Stage 6: Multi-Protocol Ingestion Job Disabler & Scheduler Sync.

Parses target batch files ('temp-data/batch_mls_targets_rets.csv', 'temp-data/batch_mls_targets_api.csv',
or 'temp-data/batch_mls_targets.csv') and disables download jobs based on 'download_protocol':
  - RETS Integration: Disables target Jenkins jobs over the Jenkins HTTP API using mls_id & mls_id_str patterns.
  - API Integration: Disables target API download jobs via production numeric mls_id payloads AND triggers Scheduler Sync.

Logs execution events to 'logs/pipeline_YYYY-MM-DD.log'.
"""

import os
import sys
import csv
import re
import signal
import logging
import requests
import warnings
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv


# --- OS-LEVEL INSTANT TERMINAL EXIT ---
def force_terminal_exit(sig=None, frame=None):
    print("\n⛔ [TERMINAL ABORT] Killing process tree immediately...")
    os._exit(1)


signal.signal(signal.SIGINT, force_terminal_exit)
if hasattr(signal, "SIGBREAK"):
    signal.signal(signal.SIGBREAK, force_terminal_exit)

warnings.filterwarnings("ignore")

current_dir = Path(__file__).resolve().parent
load_dotenv(dotenv_path=current_dir / ".env")


def setup_logger():
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    today_str = datetime.now().strftime("%Y-%m-%d")
    log_file = log_dir / f"pipeline_{today_str}.log"

    logger = logging.getLogger("Stage6_DisableIngestionJobs")
    logger.setLevel(logging.INFO)
    if not logger.hasHandlers():
        formatter = logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

        c_handler = logging.StreamHandler(sys.stdout)
        c_handler.setFormatter(formatter)
        logger.addHandler(c_handler)

        f_handler = logging.FileHandler(log_file, encoding="utf-8")
        f_handler.setFormatter(formatter)
        logger.addHandler(f_handler)

    return logger


logger = setup_logger()

# Jenkins Config (RETS)
JENKINS_URL = (os.getenv("JENKINS_PROD_URL") or os.getenv("JENKINS_STAGE_URL", "")).strip().strip("'\"")
JENKINS_USER = (os.getenv("JENKINS_USER", "")).strip().strip("'\"")
JENKINS_TOKEN = (os.getenv("JENKINS_PROD_TOKEN") or os.getenv("JENKINS_STAGE_TOKEN", "")).strip().strip("'\"")

# API Microservice Config (API)
MLS_ADMIN_PROD_URL = (os.getenv("MLS_ADMIN_PROD_URL") or os.getenv("MLS_ADMIN_STAGE_URL", "")).strip().strip(
    "'\"").rstrip("/")
PROD_BASE_URL = MLS_ADMIN_PROD_URL.split("/v1/mls-admin")[
    0] if "/v1/mls-admin" in MLS_ADMIN_PROD_URL else MLS_ADMIN_PROD_URL

API_BASE_URL = MLS_ADMIN_PROD_URL
MLS_ADMIN_API_KEY = (os.getenv("MLS_ADMIN_API_KEY") or os.getenv("MS_API_KEY", "")).strip().strip("'\"")

# Audit attribution user
UPDATED_BY_USER = os.getenv("DB_USER", "migration_pipeline_bot").strip().strip("'\"")


# ==============================================================================
# CODE BLOCK 1: RETS INGESTION JOB MANAGEMENT (JENKINS)
# ==============================================================================

def get_all_jenkins_jobs() -> list:
    """Fetch all job names and URLs from the Jenkins API."""
    if not JENKINS_URL or not JENKINS_TOKEN:
        logger.error("Missing JENKINS_PROD_URL / JENKINS_STAGE_URL or JENKINS_TOKEN in environment configuration.")
        return []

    base_url = JENKINS_URL.rstrip("/")
    url = f"{base_url}/api/json?tree=jobs[name,url]"

    try:
        response = requests.get(url, auth=(JENKINS_USER, JENKINS_TOKEN), timeout=15)
        if response.status_code == 200:
            return response.json().get("jobs", [])
        elif response.status_code == 401:
            logger.error("Jenkins Authentication failed (401). Please check JENKINS_TOKEN.")
            return []
        else:
            logger.error(f"Failed to fetch Jenkins jobs. HTTP Status: {response.status_code}")
            return []
    except Exception as err:
        logger.error(f"Error connecting to Jenkins: {err}")
        return []


def disable_jenkins_job(job_name: str) -> bool:
    """Disables a specific Jenkins job."""
    base_url = JENKINS_URL.rstrip("/")
    url = f"{base_url}/job/{job_name}/disable"

    try:
        response = requests.post(url, auth=(JENKINS_USER, JENKINS_TOKEN), timeout=15)
        if response.status_code in [200, 201, 302]:
            logger.info(f"✅ Successfully disabled Jenkins job: {job_name}")
            return True
        else:
            logger.error(f"❌ Failed to disable Jenkins job {job_name}. HTTP Status: {response.status_code}")
            return False
    except Exception as err:
        logger.error(f"Error disabling Jenkins job {job_name}: {err}")
        return False


def process_rets_disabling(rets_targets: set):
    """Handles target matching and job disabling for RETS protocol targets via Jenkins."""
    logger.info("------------------------------------------------------------")
    logger.info(f"⚙️ [RETS BLOCK] Processing {len(rets_targets)} unique target pair(s) via Jenkins...")
    logger.info("------------------------------------------------------------")

    jenkins_jobs = get_all_jenkins_jobs()
    if not jenkins_jobs:
        logger.warning("No jobs returned from Jenkins. Skipping RETS job disabling.")
        return

    matched_jobs_dict = {}
    for mls_id, mls_id_str in rets_targets:
        pattern = re.compile(rf"^{re.escape(mls_id)}-{re.escape(mls_id_str)}.*")
        for job in jenkins_jobs:
            if pattern.match(job["name"]):
                matched_jobs_dict[job["name"]] = job

    matched_jobs = list(matched_jobs_dict.values())
    logger.info(f"Found {len(matched_jobs)} matching Jenkins job(s) for RETS targets.")

    for job in matched_jobs:
        logger.info(f" - Matched Job: {job['name']} ({job['url']})")
        disable_jenkins_job(job["name"])


# ==============================================================================
# CODE BLOCK 2: API INGESTION JOB MANAGEMENT & SCHEDULER SYNC
# ==============================================================================

def trigger_scheduler_sync() -> bool:
    """Triggers Download Manager Scheduler Sync POST call."""
    if not PROD_BASE_URL:
        logger.error("Missing MLS_ADMIN_PROD_URL / MLS_ADMIN_STAGE_URL in environment configuration.")
        return False

    sync_url = f"{PROD_BASE_URL}/v1/download-manager/scheduler/sync"
    logger.info("🔄 ACTION: TRIGGER DOWNLOAD MANAGER SCHEDULER SYNC")
    logger.info(f"Target Endpoint : {sync_url}")

    headers = {
        "accept": "application/json",
        "api-key": MLS_ADMIN_API_KEY,
    }

    try:
        response = requests.post(sync_url, headers=headers, data="", timeout=15)
        if response.status_code in (200, 201, 204):
            logger.info("✅ SUCCESS: Download Manager Scheduler synchronized successfully.")
            logger.info(f"Response: {response.text}")
            return True
        else:
            logger.error(f"❌ SCHEDULER SYNC FAILED [{response.status_code}]: {response.text}")
            return False
    except Exception as e:
        logger.error(f"❌ Scheduler Sync Request Exception: {e}", exc_info=True)
        return False


def disable_api_download_jobs_batch(mls_ids: list[int]) -> bool:
    """Disables MLS Download Jobs for target sources using numeric mls_id list."""
    if not API_BASE_URL:
        logger.error("Missing MLS_ADMIN_PROD_URL / MLS_ADMIN_STAGE_URL in environment configuration.")
        return False

    endpoint = f"{API_BASE_URL}/mls/download/jobs/disable"
    params = {"temp_update": "true"}

    headers = {
        "accept": "application/json",
        "api-key": MLS_ADMIN_API_KEY,
        "Content-Type": "application/json"
    }

    payload = {
        "sources": mls_ids,
        "updated_by": UPDATED_BY_USER
    }

    logger.info(f"Target API MLS IDs ({len(mls_ids)}) : {mls_ids}")
    logger.info(f"Target Endpoint            : {endpoint}")

    try:
        response = requests.post(endpoint, params=params, headers=headers, json=payload, timeout=15)
        if response.status_code in (200, 201):
            logger.info("✅ SUCCESS: Download jobs successfully DISABLED for API targets.")
            logger.info(f"Response: {response.text}")
            return True
        else:
            logger.error(f"❌ FAILED TO DISABLE API JOBS [{response.status_code}]: {response.text}")
            return False
    except Exception as e:
        logger.error(f"❌ API Request Exception: {e}", exc_info=True)
        return False


def process_api_disabling(api_targets: set):
    """Disables API protocol targets using numeric MLS IDs and triggers mandatory Scheduler Sync."""
    logger.info("------------------------------------------------------------")
    logger.info(f"⚙️ [API BLOCK] Processing {len(api_targets)} unique target MLS(s) via API Service...")
    logger.info("------------------------------------------------------------")

    mls_ids = sorted([int(mls_id) for mls_id, _ in api_targets if mls_id.isdigit()])
    if not mls_ids:
        logger.warning("No valid numeric MLS IDs found for API targets.")
        return

    # 1. Batch disable API download jobs
    if disable_api_download_jobs_batch(mls_ids):
        # 2. Trigger mandatory Scheduler Sync post-job disable
        logger.info("------------------------------------------------------------")
        trigger_scheduler_sync()


# ==============================================================================
# MAIN ROUTING ENGINE
# ==============================================================================

def run_job_disabling():
    temp_data_dir = current_dir / "temp-data"

    target_csv_files = [
        temp_data_dir / "batch_mls_targets_rets.csv",
        temp_data_dir / "batch_mls_targets_api.csv",
        temp_data_dir / "batch_mls_targets.csv",
    ]

    rets_targets = set()
    api_targets = set()

    for csv_path in target_csv_files:
        if not csv_path.exists():
            continue

        with open(csv_path, mode="r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                mls_id = row.get("mls_id", "").strip()
                mls_id_str = row.get("mls_id_str", "").strip()
                protocol = row.get("download_protocol", "").strip().lower()

                if not mls_id or not mls_id_str:
                    continue

                if "rets" in protocol:
                    rets_targets.add((mls_id, mls_id_str))
                elif any(p in protocol for p in ["api", "rest", "oauth"]):
                    api_targets.add((mls_id, mls_id_str))
                else:
                    if "rets" in csv_path.name:
                        rets_targets.add((mls_id, mls_id_str))
                    elif "api" in csv_path.name:
                        api_targets.add((mls_id, mls_id_str))

    logger.info("============================================================")
    logger.info("🚀 STAGE 6: DISABLING INGESTION JOBS & SCHEDULER SYNC")
    logger.info("============================================================")
    logger.info(f"RETS Target Feeds Discovered : {len(rets_targets)}")
    logger.info(f"API Target Feeds Discovered  : {len(api_targets)}")
    logger.info("============================================================")

    if rets_targets:
        process_rets_disabling(rets_targets)

    if api_targets:
        process_api_disabling(api_targets)

    if not rets_targets and not api_targets:
        logger.warning("No RETS or API targets discovered in batch CSVs. No jobs were disabled.")


if __name__ == "__main__":
    try:
        run_job_disabling()
    except (KeyboardInterrupt, SystemExit):
        force_terminal_exit()