"""
Pipeline Stage 8: Multi-Protocol Ingestion Job Enabler & Trigger Engine (Parallelized & Robust).

Parses target batch files ('temp-data/batch_mls_targets_rets.csv', 'temp-data/batch_mls_targets_api.csv',
or 'temp-data/batch_mls_targets.csv') and triggers ingestion downloads in parallel:
- RETS Integration: Enables Jenkins jobs, triggers 'buildWithParameters' (LOAD_TYPE=incr),
                      and polls execution status until completion concurrently.
- API Integration: Triggers 3-day manual reprocess downloads via Production/Stage Microservice API,
                     captures batch IDs as strict strings, and logs details to
                     'temp-data/triggered_downloads_log_api.csv'.

Logs execution events to 'logs/pipeline_YYYY-MM-DD.log'.
"""

import os
import sys
import csv
import re
import time
import signal
import logging
import requests
import warnings
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
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

    logger = logging.getLogger("Stage8_TriggerDownload")
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

# ==============================================================================
# ENVIRONMENT CONSTANTS
# ==============================================================================
# Jenkins Config (RETS)
JENKINS_URL = (os.getenv("JENKINS_PROD_URL") or os.getenv("JENKINS_STAGE_URL", "")).strip().strip("'\"")
JENKINS_USER = (os.getenv("JENKINS_USER", "")).strip().strip("'\"")
JENKINS_TOKEN = (os.getenv("JENKINS_PROD_TOKEN") or os.getenv("JENKINS_STAGE_TOKEN", "")).strip().strip("'\"")

# API Microservice Config (API)
MLS_ADMIN_PROD_URL = (os.getenv("MLS_ADMIN_PROD_URL") or os.getenv("MLS_ADMIN_STAGE_URL", "")).strip().strip(
    "'\"").rstrip("/")
PROD_BASE_URL = MLS_ADMIN_PROD_URL.split("/v1/mls-admin")[
    0] if "/v1/mls-admin" in MLS_ADMIN_PROD_URL else MLS_ADMIN_PROD_URL

API_KEY = (os.getenv("MLS_ADMIN_API_KEY") or os.getenv("MS_API_KEY", "")).strip().strip("'\"")

# Jenkins Polling Config
POLL_INTERVAL_SEC = 15
MAX_WAIT_MINUTES = 10
MAX_PARALLEL_WORKERS = 8

# API Download Defaults
DEFAULT_RELATIVE_PERIOD = "3"
DEFAULT_RELATIVE_PERIOD_UNIT = "day"


# ==============================================================================
# ROBUST NETWORK SESSION BUILDER
# ==============================================================================

def create_robust_session() -> requests.Session:
    """Creates a requests session configured with automatic retries for dropped connections."""
    session = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=2,
        status_forcelist=[500, 502, 503, 504],
        raise_on_status=False
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


# ==============================================================================
# CODE BLOCK 1: RETS INGESTION TRIGGER & MONITORING (JENKINS)
# ==============================================================================

def get_jenkins_crumb(session: requests.Session) -> dict:
    """Fetches a CSRF Crumb header if required by Jenkins."""
    base_url = JENKINS_URL.rstrip("/")
    try:
        response = session.get(f"{base_url}/crumbIssuer/api/json", timeout=10)
        if response.status_code == 200:
            data = response.json()
            return {data["crumbRequestField"]: data["crumb"]}
    except Exception:
        pass
    return {}


def trigger_and_monitor_jenkins_job(session: requests.Session, job_name: str, headers: dict,
                                    load_type: str = "incr") -> bool:
    """Enables, triggers, and polls a Jenkins job until completion (Thread Worker)."""
    base_url = JENKINS_URL.rstrip("/")

    # 1. Enable Job
    try:
        enable_url = f"{base_url}/job/{job_name}/enable"
        session.post(enable_url, headers=headers, timeout=15)
        logger.info(f"✅ Enabled Jenkins job: {job_name}")
    except Exception as e:
        logger.warning(f"⚠️ Could not verify enabled status for '{job_name}': {e}")

    # 2. Trigger Parameterized Build
    build_url = f"{base_url}/job/{job_name}/buildWithParameters"
    try:
        response = session.post(build_url, headers=headers, params={"LOAD_TYPE": load_type}, timeout=15)
        if response.status_code not in (200, 201, 202, 302):
            logger.error(f"❌ Failed to trigger build for '{job_name}' [{response.status_code}]: {response.text}")
            return False
        logger.info(f"🚀 Triggered build (LOAD_TYPE={load_type}) for '{job_name}'")
    except Exception as e:
        logger.error(f"❌ Network failure while triggering '{job_name}': {e}")
        return False

    # 3. Resolve Queue Item to get Build Number
    queue_url = response.headers.get("Location")
    build_number = None

    if queue_url:
        logger.info(f"⏳ Waiting for build '{job_name}' to start in queue...")
        for _ in range(12):
            time.sleep(5)
            try:
                q_res = session.get(f"{queue_url.rstrip('/')}/api/json", timeout=10)
                if q_res.status_code == 200:
                    q_data = q_res.json()
                    executable = q_data.get("executable")
                    if executable and "number" in executable:
                        build_number = executable["number"]
                        break
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as err:
                logger.warning(f"⚠️ Connection glitch polling queue for '{job_name}' (retrying): {err}")

    # Fallback: query job API directly if queue resolution timed out
    if not build_number:
        try:
            last_build_res = session.get(f"{base_url}/job/{job_name}/api/json", timeout=10)
            if last_build_res.status_code == 200:
                build_number = last_build_res.json().get("nextBuildNumber", 1) - 1
        except Exception as e:
            logger.warning(f"⚠️ Could not fetch last build number for '{job_name}': {e}")

    if not build_number:
        logger.warning(f"⚠️ Could not determine build number for '{job_name}'. Trigger issued, skipping polling.")
        return True

    console_url = f"{base_url}/job/{job_name}/{build_number}/console"
    logger.info(f"📌 Build #{build_number} active for '{job_name}'. Live logs: {console_url}")

    # 4. Asynchronous Polling Loop
    start_time = time.time()
    max_wait_seconds = MAX_WAIT_MINUTES * 60

    while (time.time() - start_time) < max_wait_seconds:
        try:
            status_res = session.get(f"{base_url}/job/{job_name}/{build_number}/api/json", timeout=10)
            if status_res.status_code == 200:
                data = status_res.json()
                is_building = data.get("building", False)
                result = data.get("result")

                if not is_building:
                    if result == "SUCCESS":
                        logger.info(f"🎉 Build #{build_number} for '{job_name}' COMPLETED SUCCESSFULLY!")
                        return True
                    else:
                        logger.error(f"❌ Build #{build_number} for '{job_name}' FAILED! Result: {result}")
                        return False

                elapsed = int(time.time() - start_time)
                logger.info(f"⏳ Build #{build_number} ('{job_name}') in progress... (Elapsed: {elapsed}s)")
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as err:
            logger.warning(
                f"⚠️ Network blip polling status for '{job_name}': {err}. Retrying in {POLL_INTERVAL_SEC}s...")

        time.sleep(POLL_INTERVAL_SEC)

    logger.error(f"⌛ Timed out after {MAX_WAIT_MINUTES} minutes waiting for '{job_name}' build #{build_number}.")
    return False


def process_rets_triggers_parallel(rets_targets: set):
    """Handles target job matching and parallel execution for RETS feeds."""
    logger.info("------------------------------------------------------------")
    logger.info(f"⚙️ [RETS BLOCK] Processing {len(rets_targets)} unique target pair(s) via Jenkins (Parallel)...")
    logger.info("------------------------------------------------------------")

    if not JENKINS_URL or not JENKINS_TOKEN:
        logger.error("❌ Missing JENKINS_PROD_URL / JENKINS_STAGE_URL or JENKINS_TOKEN in environment configuration.")
        return

    session = create_robust_session()
    session.auth = (JENKINS_USER, JENKINS_TOKEN)
    headers = get_jenkins_crumb(session)

    list_res = session.get(f"{JENKINS_URL.rstrip('/')}/api/json?tree=jobs[name,url]", timeout=15)
    if list_res.status_code != 200:
        logger.error(f"❌ Failed to fetch Jenkins jobs. HTTP Status: {list_res.status_code}")
        return

    jenkins_jobs = list_res.json().get("jobs", [])
    matched_jobs_dict = {}

    for mls_id, mls_id_str in rets_targets:
        pattern = re.compile(rf"^{re.escape(str(mls_id))}-{re.escape(str(mls_id_str))}.*")
        for job in jenkins_jobs:
            if pattern.match(job["name"]):
                matched_jobs_dict[job["name"]] = job

    matched_jobs = list(matched_jobs_dict.values())
    logger.info(f"Found {len(matched_jobs)} matching Jenkins job(s) for RETS targets.")

    if not matched_jobs:
        return

    success_count = 0
    failure_count = 0

    with ThreadPoolExecutor(max_workers=min(len(matched_jobs), MAX_PARALLEL_WORKERS)) as executor:
        future_to_job = {
            executor.submit(trigger_and_monitor_jenkins_job, session, job["name"], headers, "incr"): job["name"]
            for job in matched_jobs
        }

        for future in as_completed(future_to_job):
            job_name = future_to_job[future]
            try:
                if future.result():
                    success_count += 1
                else:
                    failure_count += 1
            except Exception as exc:
                logger.error(f"❌ Exception in thread execution for '{job_name}': {exc}")
                failure_count += 1

    logger.info(f"✅ RETS Builds Complete | Successful: {success_count} | Failed: {failure_count}")


# ==============================================================================
# CODE BLOCK 2: API INGESTION MANUAL DOWNLOAD TRIGGER ENGINE
# ==============================================================================

def trigger_api_download(session: requests.Session, mls_id: int, mls_id_str: str, content_type: str,
                         content_sub_type: str, max_retries: int = 4) -> dict | None:
    """Triggers 3-day download for an API target with strict string batch ID casting and backoff retry logic."""
    url = f"{PROD_BASE_URL}/v1/listing-mflow/reprocess/download/mls/{mls_id}"

    headers = {
        "accept": "application/json",
        "api-key": API_KEY,
    }

    params = {
        "dl_type": "range",
        "relative_period": DEFAULT_RELATIVE_PERIOD,
        "relative_period_unit": DEFAULT_RELATIVE_PERIOD_UNIT,
        "listing_id_name": "ListingId",
        "use_in_operator": "true",
        "content_type": content_type,
        "content_sub_type": content_sub_type,
        "query_validation": "false",
        "force": "true",
    }

    logger.info(
        f"🚀 Triggering 3-Day Download | MLS: {mls_id} ({mls_id_str}) | [{content_type} / {content_sub_type}]"
    )

    for attempt in range(1, max_retries + 1):
        try:
            response = session.post(url, headers=headers, params=params, data="", timeout=30)

            if response.status_code in (200, 201, 202):
                data = response.json() if response.text else {}

                # FORCE STRICT STRING CASTING TO PREVENT 64-BIT INT FLOAT ROUNDING
                raw_batch_id = data.get("batch_id")
                batch_id = str(raw_batch_id).strip() if raw_batch_id is not None else ""

                logger.info(f"   ✅ SUCCESS [{response.status_code}]: Triggered Batch ID = {batch_id}")
                if batch_id:
                    return {
                        "mls_id": mls_id,
                        "mls_id_str": mls_id_str,
                        "content_type": content_type,
                        "content_sub_type": content_sub_type,
                        "batch_id": batch_id,
                    }
                return None

            elif response.status_code == 403:
                err = response.json().get("message", response.text) if response.text else "Forbidden"
                logger.error(f"   ❌ AUTH ERROR [403]: Invalid API key. ({err})")
                return None

            elif response.status_code in (406, 422):
                err = response.json().get("message", response.text) if response.text else "Validation error"
                logger.error(
                    f"   ❌ INPUT ERROR [{response.status_code}]: Invalid request for MLS {mls_id} "
                    f"({content_type}/{content_sub_type}). Message: {err}"
                )
                return None

            elif response.status_code == 404 and "KeyError" in response.text and attempt < max_retries:
                backoff = attempt * 2
                logger.warning(
                    f"   ⚠️ Intermittent Backend KeyError [404] for MLS {mls_id} ({content_type}). "
                    f"Retrying attempt {attempt}/{max_retries} in {backoff}s..."
                )
                time.sleep(backoff)
                continue

            else:
                logger.error(f"   ❌ UNEXPECTED ERROR [{response.status_code}]: {response.text}")
                return None

        except requests.RequestException as e:
            if attempt < max_retries:
                time.sleep(2)
                continue
            logger.error(f"   ❌ Request Exception for MLS {mls_id}: {e}", exc_info=True)
            return None

    return None


def save_triggered_downloads_log(batch_records: list[dict]) -> Path:
    """Saves batch IDs and target metadata to temp-data/triggered_downloads_log_api.csv."""
    temp_dir = current_dir / "temp-data"
    temp_dir.mkdir(parents=True, exist_ok=True)
    csv_file = temp_dir / "triggered_downloads_log_api.csv"

    fieldnames = ["mls_id", "mls_id_str", "content_type", "content_sub_type", "batch_id"]
    with open(csv_file, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(batch_records)

    logger.info(f"📄 Saved {len(batch_records)} batch trigger record(s) to '{csv_file}'.")
    return csv_file


def process_api_triggers_parallel(api_targets: list[dict]):
    """Processes 3-day manual reprocess downloads serially per MLS ID to eliminate backend thread locks."""
    logger.info("------------------------------------------------------------")
    logger.info(f"⚙️ [API BLOCK] Processing {len(api_targets)} target download payload(s) via API Service...")
    logger.info("------------------------------------------------------------")

    if not API_KEY:
        logger.error("❌ MLS_ADMIN_API_KEY / MS_API_KEY missing from .env file.")
        return

    session = create_robust_session()
    triggered_records = []
    total_triggered = 0
    total_failed = 0

    grouped_by_mls = {}
    for t in api_targets:
        grouped_by_mls.setdefault(t["mls_id"], []).append(t)

    def process_mls_group(targets_list):
        group_records = []
        for idx, t in enumerate(targets_list):
            if idx > 0:
                time.sleep(1.5)
            res = trigger_api_download(
                session,
                t["mls_id"],
                t["mls_id_str"],
                t["content_type"],
                t["content_sub_type"]
            )
            if res:
                group_records.append(res)
        return group_records

    with ThreadPoolExecutor(max_workers=min(len(grouped_by_mls), MAX_PARALLEL_WORKERS)) as executor:
        future_to_mls = {
            executor.submit(process_mls_group, targets): mls_id
            for mls_id, targets in grouped_by_mls.items()
        }

        for future in as_completed(future_to_mls):
            mls_id = future_to_mls[future]
            try:
                recs = future.result()
                if recs:
                    triggered_records.extend(recs)
                    total_triggered += len(recs)
                expected_count = len(grouped_by_mls[mls_id])
                total_failed += (expected_count - len(recs))
            except Exception as exc:
                logger.error(f"❌ Exception in group execution for MLS '{mls_id}': {exc}")
                total_failed += len(grouped_by_mls[mls_id])

    save_triggered_downloads_log(triggered_records)
    logger.info(f"✅ API Triggers Complete | Successful: {total_triggered} | Failed: {total_failed}")


# ==============================================================================
# MAIN ROUTING ENGINE
# ==============================================================================

def main():
    logger.info("============================================================")
    logger.info("🚀 STAGE 8: ENABLING INGESTION JOBS & TRIGGERING DOWNLOADS")
    logger.info("============================================================")

    temp_data_dir = current_dir / "temp-data"
    target_csv_files = [
        temp_data_dir / "batch_mls_targets_rets.csv",
        temp_data_dir / "batch_mls_targets_api.csv",
        temp_data_dir / "batch_mls_targets.csv",
    ]

    rets_targets = set()
    api_targets_dict = {}

    for csv_path in target_csv_files:
        if not csv_path.exists():
            continue

        with open(csv_path, mode="r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                mls_id_raw = row.get("mls_id", "").strip()
                mls_id_str = row.get("mls_id_str", "").strip()
                protocol = row.get("download_protocol", "").strip().lower()

                if not mls_id_raw or not mls_id_str:
                    continue

                if "rets" in protocol or "rets" in csv_path.name:
                    rets_targets.add((mls_id_raw, mls_id_str))
                elif any(p in protocol for p in ["api", "rest", "oauth"]) or "api" in csv_path.name:
                    if mls_id_raw.isdigit():
                        mls_id = int(mls_id_raw)
                        content_type = row.get("content_type", "").strip()
                        content_sub_type = row.get("content_sub_type", "").strip()
                        key = (mls_id, content_type, content_sub_type)

                        if key not in api_targets_dict:
                            api_targets_dict[key] = {
                                "mls_id": mls_id,
                                "mls_id_str": mls_id_str,
                                "content_type": content_type,
                                "content_sub_type": content_sub_type,
                            }

    api_targets = list(api_targets_dict.values())

    logger.info(f"RETS Target Feeds Discovered : {len(rets_targets)}")
    logger.info(f"API Target Feeds Discovered  : {len(api_targets)}")
    logger.info("============================================================")

    if rets_targets:
        process_rets_triggers_parallel(rets_targets)

    if api_targets:
        process_api_triggers_parallel(api_targets)

    if not rets_targets and not api_targets:
        logger.warning("No RETS or API targets discovered in batch CSVs. No downloads were triggered.")


if __name__ == "__main__":
    try:
        main()
    except (KeyboardInterrupt, SystemExit):
        force_terminal_exit()