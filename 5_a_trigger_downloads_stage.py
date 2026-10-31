"""
Pipeline Stage 5a: Stage Manual Ingestion Download Trigger & Verification Engine.

Triggers manual ingestion downloads in Stage environment for MLS IDs in 'temp-data/promotion_sources.txt':
1. Reads candidate numeric MLS IDs from 'temp-data/promotion_sources.txt'.
2. Queries Stage DB over SSH tunnel to separate active feeds (mls_status_id = 2) from inactive feeds.
3. RETS Feeds (Pre-Ingestion Preparation & Download):
   - Triggers 'update_sync_time_to_yesterdays' (SOURCE_ID=sa_mls_id, STAGE_FLAG=stage).
   - Triggers 'metadataSync-STAGE' (SOURCE_ID=sa_mls_id, SOURCE_NAME=sa_mls_id_str, LOAD_TYPE=full, STAGE_FLAG=stage).
   - Triggers active incremental ingestion runner build ({mls_id}-{mls_id_str}...) with LOAD_TYPE=incr.
4. API Feeds:
   - Triggers 3-day window downloads via Stage listing-mflow reprocess endpoint.
5. Error Isolation & Inactivity Handling:
   - Inactive feeds (mls_status_id != 2) are gracefully marked as skipped.
   - Missing Jenkins jobs or non-fatal network exceptions are recorded without halting the pipeline.
6. Outputs:
   - 'temp-data/triggered_downloads_stage_log.csv'
   - Console logs and 'logs/pipeline_YYYY-MM-DD.log'
"""

import os
import io
import sys
import csv
import re
import time
import signal
import logging
import psycopg2
import psycopg2.extras
import paramiko
import requests
import warnings
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from dotenv import load_dotenv
from sshtunnel import SSHTunnelForwarder
from cryptography.hazmat.primitives import serialization

# --- OS-LEVEL INSTANT TERMINAL EXIT ON SIGINT ---
def force_terminal_exit(sig=None, frame=None):
    print("\n⛔ [TERMINAL ABORT] Killing process tree immediately...")
    os._exit(1)


signal.signal(signal.SIGINT, force_terminal_exit)
if hasattr(signal, "SIGBREAK"):
    signal.signal(signal.SIGBREAK, force_terminal_exit)

warnings.filterwarnings("ignore")

if not hasattr(paramiko, "DSSKey"):
    paramiko.DSSKey = paramiko.PKey

current_dir = Path(__file__).resolve().parent
load_dotenv(dotenv_path=current_dir / ".env")


def setup_logger():
    """Initializes console and rotating daily file logger."""
    log_dir = current_dir / "logs"
    log_dir.mkdir(exist_ok=True)
    today_str = datetime.now().strftime("%Y-%m-%d")
    log_file = log_dir / f"pipeline_{today_str}.log"

    logger = logging.getLogger("StageDownloadsTrigger")
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
# ENVIRONMENT CONSTANTS (STAGING)
# ==============================================================================
JENKINS_STAGE_URL = (os.getenv("JENKINS_STAGE_URL") or os.getenv("JENKINS_URL", "")).strip().strip("'\"")
JENKINS_USER = (os.getenv("JENKINS_USER", "")).strip().strip("'\"")
JENKINS_STAGE_TOKEN = (os.getenv("JENKINS_STAGE_TOKEN") or os.getenv("JENKINS_TOKEN", "")).strip().strip("'\"")

MLS_ADMIN_STAGE_URL = (os.getenv("MLS_ADMIN_STAGE_URL") or os.getenv("MLS_ADMIN_PROD_URL", "")).strip().strip(
    "'\""
).rstrip("/")
STAGE_BASE_URL = (
    MLS_ADMIN_STAGE_URL.split("/v1/mls-admin")[0]
    if "/v1/mls-admin" in MLS_ADMIN_STAGE_URL
    else MLS_ADMIN_STAGE_URL
)

API_KEY = (os.getenv("MLS_ADMIN_API_KEY") or os.getenv("MS_API_KEY", "")).strip().strip("'\"")

POLL_INTERVAL_SEC = 15
MAX_WAIT_MINUTES = 10
MAX_PARALLEL_WORKERS = 8

DEFAULT_RELATIVE_PERIOD = "3"
DEFAULT_RELATIVE_PERIOD_UNIT = "day"


def create_robust_session() -> requests.Session:
    """Configures requests session with automatic backoff retry logic."""
    session = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=2,
        status_forcelist=[500, 502, 503, 504],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def load_ssh_pkey(key_path: str, passphrase: str):
    """Loads private RSA key from filesystem for SSH authentication."""
    with open(key_path, "rb") as key_file:
        private_key = serialization.load_pem_private_key(
            key_file.read(),
            password=passphrase.encode() if passphrase else None,
        )
    pem_data = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.TraditionalOpenSSL,
        encryption_algorithm=serialization.NoEncryption(),
    )
    return paramiko.RSAKey.from_private_key(io.StringIO(pem_data.decode()))


def load_promotion_sources_ids() -> list[int]:
    """Loads unique numeric MLS IDs from temp-data/promotion_sources.txt."""
    file_paths = [
        current_dir / "temp-data" / "promotion_sources.txt",
        current_dir / "promotion_sources.txt",
    ]
    target_ids = set()
    for path in file_paths:
        if path.exists():
            for line in path.read_text(encoding="utf-8").splitlines():
                cleaned = line.split("#")[0].strip()
                if cleaned.isdigit():
                    target_ids.add(int(cleaned))
            logger.info(f"📋 Loaded {len(target_ids)} MLS ID(s) from '{path}'.")
            return sorted(list(target_ids))
    logger.warning("⚠️ 'promotion_sources.txt' not found! Skipping triggers.")
    return []


def fetch_stage_mls_metadata(mls_ids: list[int]) -> tuple[list[dict], list[dict]]:
    """Fetches feed metadata and separates active feeds (mls_status_id = 2) from inactive feeds."""
    if not mls_ids:
        return [], []

    ssh_host = (os.getenv("STAGE_SSH_HOST") or os.getenv("REMOTE_SSH_HOST") or os.getenv("SSH_HOST", "")).strip("'\"")
    ssh_user = (os.getenv("SSH_USER") or os.getenv("REMOTE_SSH_USER", "")).strip("'\"")
    ssh_key_path = (os.getenv("SSH_KEY_PATH") or os.getenv("REMOTE_SSH_KEY_PATH", "")).strip("'\"")
    ssh_passphrase = (os.getenv("SSH_KEY_PASSPHRASE") or os.getenv("REMOTE_SSH_KEY_PASSPHRASE", "")).strip("'\"")

    db_host = (os.getenv("STAGE_DB_HOST") or os.getenv("DB_HOST", "")).strip("'\"")
    db_name = (os.getenv("DB_NAME", "mls_admin")).strip("'\"")
    db_user = (os.getenv("DB_USER", "")).strip("'\"")
    db_pass = (os.getenv("DB_PASSWORD", "")).strip("'\"")

    logger.info("🔍 Connecting to Stage DB to resolve feed metadata for promotion sources...")
    active_records = []
    inactive_records = []

    try:
        pkey = load_ssh_pkey(ssh_key_path, ssh_passphrase)
        with SSHTunnelForwarder(
            (ssh_host, 22),
            ssh_username=ssh_user,
            ssh_pkey=pkey,
            remote_bind_address=(db_host, 5432),
        ) as tunnel:
            with psycopg2.connect(
                dbname=db_name,
                user=db_user,
                password=db_pass,
                host="127.0.0.1",
                port=tunnel.local_bind_port,
            ) as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                    query = """
                        SELECT DISTINCT 
                            m.id AS mls_id, 
                            m.id_str AS mls_id_str, 
                            m.sa_mls_id,
                            m.sa_mls_id_str,
                            m.mls_status_id,
                            dp.name AS download_protocol, 
                            ctm.content_type, 
                            ctm.content_sub_type 
                        FROM public.mls m 
                        JOIN public.mls_resource mr ON mr.mls_id = m.id 
                        JOIN public.content_type_map ctm ON mr.content_type_map_id = ctm.id 
                        JOIN public.download_mls dm ON m.id = dm.mls_id 
                        JOIN public.download_config dc ON dm.download_config_id = dc.id 
                        JOIN public.download_protocol dp ON dc.download_protocol_id = dp.id 
                        WHERE m.id = ANY(%s);
                    """
                    cur.execute(query, (mls_ids,))
                    for row in cur.fetchall():
                        item = dict(row)
                        if item.get("mls_status_id") == 2:
                            active_records.append(item)
                        else:
                            inactive_records.append(item)

        logger.info(f"✅ Active Feeds Discovered (mls_status_id = 2) : {len(active_records)}")
        if inactive_records:
            logger.info(f"ℹ️ Inactive Feeds Discovered : {len(inactive_records)} (Will be recorded as skipped)")
            for in_rec in inactive_records:
                logger.info(
                    f"   └── MLS {in_rec['mls_id']} ({in_rec['mls_id_str']}) Status ID: {in_rec['mls_status_id']} -> SKIPPED"
                )

    except Exception as e:
        logger.warning(f"⚠️ Non-fatal DB query error fetching metadata: {e}. Pipeline will continue.")

    return active_records, inactive_records


# ==============================================================================
# JENKINS RETS TRIGGER & MONITORING
# ==============================================================================

def get_jenkins_crumb(session: requests.Session) -> dict:
    """Retrieves Jenkins CSRF crumb header."""
    base_url = JENKINS_STAGE_URL.rstrip("/")
    try:
        response = session.get(f"{base_url}/crumbIssuer/api/json", timeout=10)
        if response.status_code == 200:
            data = response.json()
            return {data["crumbRequestField"]: data["crumb"]}
    except Exception:
        pass
    return {}


def trigger_and_monitor_jenkins_job(
    session: requests.Session, job_name: str, headers: dict, build_params: dict
) -> tuple[bool, str]:
    """Enables, triggers, and polls a Jenkins job until build completion."""
    base_url = JENKINS_STAGE_URL.rstrip("/")

    # 1. Enable job
    try:
        enable_url = f"{base_url}/job/{job_name}/enable"
        session.post(enable_url, headers=headers, timeout=15)
        logger.info(f"✅ Enabled Jenkins job: {job_name}")
    except Exception as e:
        logger.warning(f"⚠️ Could not verify enabled status for '{job_name}': {e}")

    # 2. Trigger parameterized build
    build_url = f"{base_url}/job/{job_name}/buildWithParameters"
    try:
        response = session.post(build_url, headers=headers, params=build_params, timeout=15)
        if response.status_code not in (200, 201, 202, 302):
            logger.warning(
                f"⚠️ Could not trigger Stage build for '{job_name}' [{response.status_code}]: {response.text[:100]}"
            )
            return False, f"HTTP_{response.status_code}"
        logger.info(f"🚀 Triggered Stage build for '{job_name}' with params: {build_params}")
    except Exception as e:
        logger.warning(f"⚠️ Network failure while triggering '{job_name}': {e}")
        return False, "NETWORK_ERROR"

    # 3. Resolve Queue Location to Build Number
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
            except Exception:
                pass

    if not build_number:
        try:
            last_build_res = session.get(f"{base_url}/job/{job_name}/api/json", timeout=10)
            if last_build_res.status_code == 200:
                build_number = last_build_res.json().get("nextBuildNumber", 1) - 1
        except Exception as e:
            logger.warning(f"⚠️ Could not fetch last build number for '{job_name}': {e}")

    if not build_number:
        logger.warning(f"⚠️ Could not determine build number for '{job_name}'. Skipping polling.")
        return True, "TRIGGERED_UNPOLLED"

    console_url = f"{base_url}/job/{job_name}/{build_number}/console"
    logger.info(f"📌 Build #{build_number} active for '{job_name}'. Live logs: {console_url}")

    # 4. Poll until completion
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
                        logger.info(f"🎉 Stage Build #{build_number} for '{job_name}' COMPLETED SUCCESSFULLY!")
                        return True, "SUCCESS"
                    else:
                        logger.warning(f"⚠️ Stage Build #{build_number} for '{job_name}' finished with result: {result}")
                        return False, f"BUILD_RESULT_{result}"

                elapsed = int(time.time() - start_time)
                logger.info(f"⏳ Build #{build_number} ('{job_name}') in progress... (Elapsed: {elapsed}s)")
        except Exception as err:
            logger.warning(f"⚠️ Network blip polling status for '{job_name}': {err}. Retrying in {POLL_INTERVAL_SEC}s...")

        time.sleep(POLL_INTERVAL_SEC)

    logger.warning(f"⚠️ Timed out after {MAX_WAIT_MINUTES} minutes waiting for '{job_name}' build #{build_number}.")
    return False, "POLL_TIMEOUT"


def execute_rets_feed_pipeline(
    session: requests.Session, headers: dict, rets_meta: dict, jenkins_jobs: list
) -> list[dict]:
    """
    Executes the 3-step sequence for an active RETS feed:
      Step 1: update_sync_time_to_yesterdays (SOURCE_ID=sa_mls_id, STAGE_FLAG=stage)
      Step 2: metadataSync-STAGE (SOURCE_ID=sa_mls_id, SOURCE_NAME=sa_mls_id_str, LOAD_TYPE=full, STAGE_FLAG=stage)
      Step 3: {mls_id}-{mls_id_str}-... runner job (LOAD_TYPE=incr)
    """
    mls_id = rets_meta["mls_id"]
    mls_id_str = rets_meta["mls_id_str"]
    sa_mls_id = str(rets_meta.get("sa_mls_id") or "").strip()
    sa_mls_id_str = str(rets_meta.get("sa_mls_id_str") or "").strip()

    log_entries = []

    logger.info(f"\n▶️ Starting Pre-Ingestion Sequence for RETS MLS {mls_id} ({mls_id_str}) | SA ID: {sa_mls_id}...")

    # --- STEP 1: update_sync_time_to_yesterdays ---
    if sa_mls_id:
        logger.info(f"[1/3] Triggering 'update_sync_time_to_yesterdays' for SOURCE_ID={sa_mls_id}...")
        ok_sync, status_sync = trigger_and_monitor_jenkins_job(
            session=session,
            job_name="update_sync_time_to_yesterdays",
            headers=headers,
            build_params={"SOURCE_ID": sa_mls_id, "STAGE_FLAG": "stage"},
        )
        log_entries.append({
            "mls_id": mls_id, "mls_id_str": mls_id_str, "protocol": "RETS",
            "content_type": "all", "content_sub_type": "all",
            "batch_id": "update_sync_time_to_yesterdays", "status": status_sync,
        })
    else:
        logger.warning(f"⚠️ No sa_mls_id found for MLS {mls_id}. Skipping 'update_sync_time_to_yesterdays'.")

    # --- STEP 2: metadataSync-STAGE ---
    if sa_mls_id and sa_mls_id_str:
        logger.info(f"[2/3] Triggering 'metadataSync-STAGE' for SOURCE_ID={sa_mls_id}, SOURCE_NAME={sa_mls_id_str}...")
        ok_meta, status_meta = trigger_and_monitor_jenkins_job(
            session=session,
            job_name="metadataSync-STAGE",
            headers=headers,
            build_params={
                "SOURCE_ID": sa_mls_id,
                "SOURCE_NAME": sa_mls_id_str,
                "LOAD_TYPE": "full",
                "STAGE_FLAG": "stage",
            },
        )
        log_entries.append({
            "mls_id": mls_id, "mls_id_str": mls_id_str, "protocol": "RETS",
            "content_type": "all", "content_sub_type": "all",
            "batch_id": "metadataSync-STAGE", "status": status_meta,
        })
    else:
        logger.warning(f"⚠️ Incomplete SA identifiers for MLS {mls_id}. Skipping 'metadataSync-STAGE'.")

    # --- STEP 3: Ingestion Runner Job ({mls_id}-{mls_id_str}-...) ---
    pattern = re.compile(rf"^{re.escape(str(mls_id))}-{re.escape(str(mls_id_str))}.*")
    matched_runner_jobs = [j["name"] for j in jenkins_jobs if pattern.match(j["name"])]

    if matched_runner_jobs:
        for runner_job in matched_runner_jobs:
            logger.info(f"[3/3] Triggering Ingestion Download: '{runner_job}' (LOAD_TYPE=incr)...")
            ok_runner, status_runner = trigger_and_monitor_jenkins_job(
                session=session,
                job_name=runner_job,
                headers=headers,
                build_params={"LOAD_TYPE": "incr"},
            )
            log_entries.append({
                "mls_id": mls_id, "mls_id_str": mls_id_str, "protocol": "RETS",
                "content_type": "all", "content_sub_type": "all",
                "batch_id": runner_job, "status": status_runner,
            })
    else:
        logger.warning(f"⚠️ SKIPPED | No ingestion runner Jenkins job found on Stage for RETS MLS {mls_id} ({mls_id_str})")
        log_entries.append({
            "mls_id": mls_id, "mls_id_str": mls_id_str, "protocol": "RETS",
            "content_type": "all", "content_sub_type": "all",
            "batch_id": "", "status": "SKIPPED_NO_JENKINS_JOB",
        })

    return log_entries


def process_rets_triggers_parallel(active_rets_records: list[dict]) -> list[dict]:
    """Processes RETS feeds concurrently with pre-ingestion sync and metadata jobs."""
    logger.info("------------------------------------------------------------")
    logger.info(f"⚙️ [RETS BLOCK] Processing {len(active_rets_records)} RETS feed(s) via Stage Jenkins...")
    logger.info("------------------------------------------------------------")

    log_records = []
    if not JENKINS_STAGE_URL or not JENKINS_STAGE_TOKEN:
        logger.warning("⚠️ Missing JENKINS_STAGE_URL or JENKINS_STAGE_TOKEN in .env. Skipping RETS triggers.")
        for r in active_rets_records:
            log_records.append({
                "mls_id": r["mls_id"], "mls_id_str": r["mls_id_str"], "protocol": "RETS",
                "content_type": "all", "content_sub_type": "all", "batch_id": "", "status": "SKIPPED_NO_JENKINS_ENV",
            })
        return log_records

    session = create_robust_session()
    session.auth = (JENKINS_USER, JENKINS_STAGE_TOKEN)
    headers = get_jenkins_crumb(session)

    try:
        list_res = session.get(f"{JENKINS_STAGE_URL.rstrip('/')}/api/json?tree=jobs[name,url]", timeout=15)
        jenkins_jobs = list_res.json().get("jobs", []) if list_res.status_code == 200 else []
    except Exception as e:
        logger.warning(f"⚠️ Could not connect to Jenkins: {e}. Skipping RETS triggers.")
        jenkins_jobs = []

    # Group by MLS to avoid duplicate execution on multi-mapping rows
    unique_rets_feeds = {}
    for r in active_rets_records:
        if r["mls_id"] not in unique_rets_feeds:
            unique_rets_feeds[r["mls_id"]] = r

    with ThreadPoolExecutor(max_workers=min(len(unique_rets_feeds), MAX_PARALLEL_WORKERS)) as executor:
        future_to_feed = {
            executor.submit(execute_rets_feed_pipeline, session, headers, feed_meta, jenkins_jobs): feed_meta["mls_id"]
            for feed_meta in unique_rets_feeds.values()
        }

        for future in as_completed(future_to_feed):
            feed_mls_id = future_to_feed[future]
            try:
                entries = future.result()
                if entries:
                    log_records.extend(entries)
            except Exception as exc:
                logger.error(f"❌ Exception in RETS pipeline execution for MLS {feed_mls_id}: {exc}")

    return log_records


# ==============================================================================
# API INGESTION TRIGGER
# ==============================================================================

def trigger_api_download(
    session: requests.Session,
    mls_id: int,
    mls_id_str: str,
    content_type: str,
    content_sub_type: str,
    max_retries: int = 2,
) -> dict:
    """Sends API request to trigger listing-mflow reprocess download."""
    url = f"{STAGE_BASE_URL}/v1/listing-mflow/reprocess/download/mls/{mls_id}"

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
        f"🚀 Triggering 3-Day Stage Download | MLS: {mls_id} ({mls_id_str}) | [{content_type} / {content_sub_type}]"
    )

    for attempt in range(1, max_retries + 1):
        try:
            response = session.post(url, headers=headers, params=params, data="", timeout=30)

            if response.status_code in (200, 201, 202):
                data = response.json() if response.text else {}
                raw_batch_id = data.get("batch_id")
                batch_id = str(raw_batch_id).strip() if raw_batch_id is not None else ""

                logger.info(f"   ✅ SUCCESS [{response.status_code}]: Triggered Batch ID = {batch_id}")
                return {
                    "mls_id": mls_id,
                    "mls_id_str": mls_id_str,
                    "protocol": "API",
                    "content_type": content_type,
                    "content_sub_type": content_sub_type,
                    "batch_id": batch_id,
                    "status": "SUCCESS",
                }
            else:
                logger.warning(f"   ⚠️ SKIPPED / UNABLE TO TRIGGER [{response.status_code}]: {response.text[:150]}")
                return {
                    "mls_id": mls_id,
                    "mls_id_str": mls_id_str,
                    "protocol": "API",
                    "content_type": content_type,
                    "content_sub_type": content_sub_type,
                    "batch_id": "",
                    "status": f"SKIPPED_HTTP_{response.status_code}",
                }

        except requests.RequestException as e:
            if attempt < max_retries:
                time.sleep(2)
                continue
            logger.warning(f"   ⚠️ Request Exception for MLS {mls_id}: {e}")
            return {
                "mls_id": mls_id,
                "mls_id_str": mls_id_str,
                "protocol": "API",
                "content_type": content_type,
                "content_sub_type": content_sub_type,
                "batch_id": "",
                "status": "FAILED_EXCEPTION",
            }


def process_api_triggers_parallel(api_targets: list[dict], inactive_records: list[dict]) -> list[dict]:
    """Triggers API downloads in parallel for active targets while logging inactive ones as skipped."""
    logger.info("------------------------------------------------------------")
    logger.info(f"⚙️ [API BLOCK] Processing {len(api_targets)} Stage API download payload(s)...")
    logger.info("------------------------------------------------------------")

    triggered_records = []

    for in_rec in inactive_records:
        triggered_records.append({
            "mls_id": in_rec["mls_id"],
            "mls_id_str": in_rec["mls_id_str"],
            "protocol": in_rec.get("download_protocol", "API"),
            "content_type": in_rec.get("content_type", ""),
            "content_sub_type": in_rec.get("content_sub_type", ""),
            "batch_id": "",
            "status": f"SKIPPED_INACTIVE_STATUS_{in_rec.get('mls_status_id')}",
        })

    if not API_KEY:
        logger.warning("⚠️ MLS_ADMIN_API_KEY / MS_API_KEY missing from .env. Skipping API triggers.")
        for t in api_targets:
            triggered_records.append({
                "mls_id": t["mls_id"],
                "mls_id_str": t["mls_id_str"],
                "protocol": "API",
                "content_type": t["content_type"],
                "content_sub_type": t["content_sub_type"],
                "batch_id": "",
                "status": "SKIPPED_NO_API_KEY",
            })
    else:
        session = create_robust_session()
        total_triggered = 0
        total_skipped = len(inactive_records)

        grouped_by_mls = {}
        for t in api_targets:
            grouped_by_mls.setdefault(t["mls_id"], []).append(t)

        def process_mls_group(targets_list):
            group_records = []
            for idx, t in enumerate(targets_list):
                if idx > 0:
                    time.sleep(1.5)
                res = trigger_api_download(
                    session, t["mls_id"], t["mls_id_str"], t["content_type"], t["content_sub_type"]
                )
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
                        for r in recs:
                            if r.get("status") == "SUCCESS":
                                total_triggered += 1
                            else:
                                total_skipped += 1
                except Exception as exc:
                    logger.warning(f"⚠️ Exception for MLS '{mls_id}': {exc}")

        logger.info(f"✅ Stage API Triggers Complete | Successful: {total_triggered} | Skipped/Failed: {total_skipped}")

    return triggered_records


# ==============================================================================
# MAIN ENGINE
# ==============================================================================

def main():
    logger.info("============================================================")
    logger.info("🚀 STAGE 5a: TRIGGERING STAGE INGESTION DOWNLOADS")
    logger.info("============================================================")

    promotion_ids = load_promotion_sources_ids()
    if not promotion_ids:
        return

    active_records, inactive_records = fetch_stage_mls_metadata(promotion_ids)

    active_rets_records = []
    api_targets_dict = {}

    for row in active_records:
        protocol = str(row.get("download_protocol", "")).lower()

        if "rets" in protocol:
            active_rets_records.append(row)
        else:
            mls_id = row["mls_id"]
            mls_id_str = row["mls_id_str"]
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

    logger.info("============================================================")
    logger.info(f"Total Promotion Source MLS IDs : {len(promotion_ids)}")
    logger.info(f"Active RETS Feeds             : {len(active_rets_records)}")
    logger.info(f"Active API Feeds              : {len(api_targets)}")
    logger.info(f"Inactive / Skipped Feeds      : {len(inactive_records)}")
    logger.info("============================================================")

    all_log_records = []

    if active_rets_records:
        rets_logs = process_rets_triggers_parallel(active_rets_records)
        if rets_logs:
            all_log_records.extend(rets_logs)

    if api_targets or inactive_records:
        api_logs = process_api_triggers_parallel(api_targets, inactive_records)
        if api_logs:
            all_log_records.extend(api_logs)

    temp_dir = current_dir / "temp-data"
    temp_dir.mkdir(parents=True, exist_ok=True)
    csv_file = temp_dir / "triggered_downloads_stage_log.csv"

    fieldnames = ["mls_id", "mls_id_str", "protocol", "content_type", "content_sub_type", "batch_id", "status"]
    with open(csv_file, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_log_records)

    logger.info(f"📄 Saved combined trigger log to '{csv_file}'.")
    logger.info("🎉 Stage download triggering sequence finished.")


if __name__ == "__main__":
    try:
        main()
    except (KeyboardInterrupt, SystemExit):
        force_terminal_exit()
    except Exception as e:
        logger.warning(f"⚠️ Unhandled top-level exception caught gracefully: {e}")
        sys.exit(0)