"""
Pipeline Stage 9: Ingestion Data Verifier & API Scheduler Re-Enabler.

1. Reads batch trigger records from 'temp-data/triggered_downloads_log_api.csv'.
2. Queries BigQuery production landing tables (`stream-listing-prod.mls_download.<content_type>`)
   using `batch_id` with polling retries to accommodate BigQuery streaming buffer flushes.
3. Re-enables scheduled API download jobs for clean MLS targets via Production/Stage Microservice API.
4. Triggers Download Manager Scheduler Sync.

Logs execution events to 'logs/pipeline_YYYY-MM-DD.log'.
"""

import os
import sys
import csv
import time
import signal
import logging
import requests
import warnings
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
from google.cloud import bigquery


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

    logger = logging.getLogger("Stage9_VerifyAndEnableAPI")
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

# Locate GCP Credentials File if present
gcp_creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "").strip().strip("'\"")
if gcp_creds_path and not Path(gcp_creds_path).is_absolute():
    gcp_creds_path = str(current_dir / gcp_creds_path)

if gcp_creds_path and os.path.exists(gcp_creds_path):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = gcp_creds_path

# ==============================================================================
# ENVIRONMENT CONSTANTS
# ==============================================================================
MLS_ADMIN_PROD_URL = (os.getenv("MLS_ADMIN_PROD_URL") or os.getenv("MLS_ADMIN_STAGE_URL", "")).strip().strip(
    "'\"").rstrip("/")
PROD_BASE_URL = MLS_ADMIN_PROD_URL.split("/v1/mls-admin")[
    0] if "/v1/mls-admin" in MLS_ADMIN_PROD_URL else MLS_ADMIN_PROD_URL
API_KEY = (os.getenv("MLS_ADMIN_API_KEY") or os.getenv("MS_API_KEY", "")).strip().strip("'\"")

# Audit attribution
UPDATED_BY_USER = os.getenv("DB_USER", "migration_pipeline_bot").strip().strip("'\"")

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "stream-listing-prod").strip().strip("'\"")
BQ_DATASET = "mls_download"


# ==============================================================================
# BIGQUERY VERIFICATION ENGINE
# ==============================================================================

def verify_batch_landing_bq(client: bigquery.Client, content_type: str, mls_id: int, batch_id: str,
                            max_attempts: int = 5, sleep_sec: int = 10) -> int:
    """Queries BigQuery to verify rows landed for a batch ID, polling to allow streaming buffer flushes."""
    table_name = "listing" if content_type.lower() == "listing" else "open_house"
    table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{table_name}"

    query = f"""
        SELECT COUNT(1) AS row_count
        FROM `{table_id}`
        WHERE CAST(batch_id AS STRING) = '{batch_id}'
          AND mls_id = {mls_id}
    """

    logger.info(f"📦 Querying `{table_id}` | MLS: {mls_id} | batch_id: {batch_id}")

    for attempt in range(1, max_attempts + 1):
        try:
            query_job = client.query(query)
            results = list(query_job.result())
            row_count = results[0].row_count if results else 0

            if row_count > 0:
                logger.info(f"   ✅ SUCCESS: Found {row_count} row(s) for Batch ID {batch_id} in `{table_id}`.")
                return row_count

            if attempt < max_attempts:
                time.sleep(sleep_sec)

        except Exception as e:
            logger.warning(f"   ⚠️ BigQuery Query Exception on attempt {attempt}/{max_attempts}: {e}")
            if attempt < max_attempts:
                time.sleep(sleep_sec)

    logger.warning(f"   ⚠️ PENDING/EMPTY: No rows found yet for Batch ID {batch_id} in `{table_id}`.")
    return 0


# ==============================================================================
# API SCHEDULER RESTORATION ENGINE
# ==============================================================================

def enable_api_jobs(session: requests.Session, mls_ids: list[int]) -> bool:
    """Re-enables scheduled API download jobs for clean MLS IDs."""
    url = f"{PROD_BASE_URL}/v1/mls-admin/mls/download/jobs/enable"
    headers = {
        "accept": "application/json",
        "api-key": API_KEY,
        "Content-Type": "application/json"
    }

    payload = {
        "mls_ids": mls_ids,
        "updated_by": UPDATED_BY_USER
    }

    try:
        response = session.post(url, headers=headers, json=payload, timeout=30)
        if response.status_code in (200, 201, 202):
            logger.info(f"✅ SUCCESS: Re-enabled API jobs for clean MLS IDs: {mls_ids}")
            return True
        else:
            logger.error(f"❌ Failed to enable API jobs [{response.status_code}]: {response.text}")
            return False
    except Exception as e:
        logger.error(f"❌ Exception enabling API jobs for MLS IDs {mls_ids}: {e}")
        return False


def sync_download_manager_scheduler(session: requests.Session) -> bool:
    """Triggers Download Manager Scheduler Sync."""
    url = f"{PROD_BASE_URL}/v1/download-manager/scheduler/sync"
    headers = {
        "accept": "application/json",
        "api-key": API_KEY,
    }

    logger.info("🔄 ACTION: TRIGGER DOWNLOAD MANAGER SCHEDULER SYNC")
    try:
        response = session.post(url, headers=headers, data="", timeout=30)
        if response.status_code in (200, 201, 202):
            logger.info("✅ SUCCESS: Download Manager Scheduler synchronized successfully.")
            return True
        else:
            logger.error(f"❌ Scheduler Sync Failed [{response.status_code}]: {response.text}")
            return False
    except Exception as e:
        logger.error(f"❌ Exception triggering Scheduler Sync: {e}")
        return False


# ==============================================================================
# MAIN ROUTING ENGINE
# ==============================================================================

def main():
    logger.info("============================================================")
    logger.info("🚀 STAGE 9: VERIFYING DOWNLOADS & RE-ENABLING API JOBS")
    logger.info("============================================================")

    triggered_log_path = current_dir / "temp-data" / "triggered_downloads_log_api.csv"

    if not triggered_log_path.exists():
        logger.warning("No API trigger log found ('temp-data/triggered_downloads_log_api.csv'). Skipping Stage 9.")
        return

    triggered_records = []
    with open(triggered_log_path, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("mls_id") and row.get("batch_id"):
                triggered_records.append({
                    "mls_id": int(row["mls_id"].strip()),
                    "mls_id_str": row.get("mls_id_str", "").strip(),
                    "content_type": row.get("content_type", "").strip(),
                    "content_sub_type": row.get("content_sub_type", "").strip(),
                    "batch_id": str(row["batch_id"]).strip(),
                })

    if not triggered_records:
        logger.warning("Trigger log CSV is empty. No API batch IDs to verify.")
        return

    # 1. BigQuery Data Landing Verification
    try:
        bq_client = bigquery.Client(project=GCP_PROJECT_ID)
    except Exception as err:
        logger.error(f"❌ Failed to initialize BigQuery Client: {err}", exc_info=True)
        return

    mls_verified_map = {}

    logger.info("============================================================")
    logger.info("🔍 CHECKING PRODUCTION DOWNLOAD TABLES BY BATCH ID (API)")
    logger.info("============================================================")

    for rec in triggered_records:
        mls_id = rec["mls_id"]
        if mls_id not in mls_verified_map:
            mls_verified_map[mls_id] = False

        rows_found = verify_batch_landing_bq(
            bq_client,
            rec["content_type"],
            mls_id,
            rec["batch_id"]
        )

        if rows_found > 0:
            mls_verified_map[mls_id] = True

    clean_mls_ids = [mls_id for mls_id, verified in mls_verified_map.items() if verified]

    logger.info("============================================================")
    logger.info(f"✅ CLEAN & VERIFIED API TARGETS ({len(clean_mls_ids)}): {clean_mls_ids}")

    if not clean_mls_ids:
        logger.error("❌ No API targets were verified in BigQuery. Skipping job restoration.")
        return

    # 2. Re-enable API Scheduled Jobs
    session = requests.Session()
    if enable_api_jobs(session, clean_mls_ids):
        sync_download_manager_scheduler(session)

    logger.info("\n🎉 Stage 9 completed successfully! Scheduled API jobs are restored.")


if __name__ == "__main__":
    try:
        main()
    except (KeyboardInterrupt, SystemExit):
        force_terminal_exit()