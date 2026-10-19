"""
Pipeline Stage 10: Multi-Environment Legacy Rule Soft-Deletion Engine (Interactive & Gated).

Verifies UI process map dependencies across both Staging and Production databases first.
Prompts sequentially: Stage soft-deletion happens FIRST.
If Stage fails or is skipped, Production soft-deletion is AUTOMATICALLY SKIPPED for safety.

API Endpoint:
  DELETE /v1/mls-admin/rules?name=<rule_name>&deleted_by=<user>

Usage:
  python 10_soft_delete_legacy_rules.py
"""

import os
import io
import sys
import csv
import signal
import logging
import requests
import warnings
import psycopg2
import paramiko
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
from sshtunnel import SSHTunnelForwarder
from cryptography.hazmat.primitives import serialization


# --- OS-LEVEL INSTANT TERMINAL EXIT ---
def force_terminal_exit(sig=None, frame=None):
    print("\n⛔ [TERMINAL ABORT] Killing process tree immediately...")
    os._exit(1)


signal.signal(signal.SIGINT, force_terminal_exit)
if hasattr(signal, "SIGBREAK"):
    signal.signal(signal.SIGBREAK, force_terminal_exit)

warnings.filterwarnings("ignore")

if not hasattr(paramiko, 'DSSKey'):
    paramiko.DSSKey = paramiko.PKey

current_dir = Path(__file__).resolve().parent
load_dotenv(dotenv_path=current_dir / ".env")


def setup_logger():
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    today_str = datetime.now().strftime("%Y-%m-%d")
    log_file = log_dir / f"pipeline_{today_str}.log"

    logger = logging.getLogger("Stage10_SoftDeleteLegacyRules")
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


def load_legacy_candidates(temp_data_dir: Path) -> list[str]:
    """Loads legacy rule candidate names from consolidation_blueprint.csv and safe_to_delete.csv."""
    candidates = set()

    # 1. Read consolidation_blueprint.csv
    blueprint_path = temp_data_dir / "consolidation_blueprint.csv"
    if blueprint_path.exists():
        with open(blueprint_path, mode="r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                old_name = row.get("old_name", "").strip()
                if old_name:
                    candidates.add(old_name)

    # 2. Read safe_to_delete.csv (unmapped rules)
    safe_to_delete_path = temp_data_dir / "safe_to_delete.csv"
    if safe_to_delete_path.exists():
        with open(safe_to_delete_path, mode="r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                rule_name = row.get("rule_name_to_delete", "").strip()
                if rule_name:
                    candidates.add(rule_name)

    return sorted(list(candidates))


def load_ssh_pkey(key_path: str, passphrase: str):
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


def get_active_rule_usage_counts_db(env_prefix: str, rule_names: list[str]) -> dict[str, int]:
    """
    Connects over SSH tunnel to check if rules are referenced by ANY active MLS mapping
    in the specified environment (STAGE or PROD).
    """
    ssh_host = (os.getenv(f"{env_prefix}_SSH_HOST") or os.getenv("REMOTE_SSH_HOST") or os.getenv("SSH_HOST", "")).strip(
        "'\"")
    ssh_user = (os.getenv("SSH_USER") or os.getenv("REMOTE_SSH_USER", "")).strip("'\"")
    ssh_key_path = (os.getenv("SSH_KEY_PATH") or os.getenv("REMOTE_SSH_KEY_PATH", "")).strip("'\"")
    ssh_passphrase = (os.getenv("SSH_KEY_PASSPHRASE") or os.getenv("REMOTE_SSH_KEY_PASSPHRASE", "")).strip("'\"")

    db_host = (os.getenv(f"{env_prefix}_DB_HOST") or os.getenv("DB_HOST", "")).strip("'\"")
    db_name = (os.getenv("DB_NAME", "mls_admin")).strip("'\"")
    db_user = (os.getenv("DB_USER", "")).strip("'\"")
    db_pass = (os.getenv("DB_PASSWORD", "")).strip("'\"")

    usage_counts = {name: 0 for name in rule_names}
    lowercased_targets = [name.lower() for name in rule_names]

    if not ssh_host or not ssh_key_path:
        logger.error(f"❌ SSH configuration missing for {env_prefix} environment.")
        return usage_counts

    try:
        pkey = load_ssh_pkey(ssh_key_path, ssh_passphrase)
        logger.info(f"Connecting to {env_prefix} DB ({db_host}) over SSH to verify active UI map references...")

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
                with conn.cursor() as cur:
                    query = """
                        SELECT pr.name, COUNT(mra.id) AS active_refs
                        FROM public.process_rule pr
                        JOIN public.map_rule_association mra ON pr.id = mra.process_rule_id
                        JOIN public.mls_resource mr ON mra.process_map_id = mr.process_map_id
                        JOIN public.mls m ON mr.mls_id = m.id
                        WHERE pr.deleted_at IS NULL 
                          AND m.mls_status_id = 2
                          AND LOWER(pr.name) = ANY(%s)
                        GROUP BY pr.name;
                    """
                    cur.execute(query, (lowercased_targets,))
                    for rule_name, count in cur.fetchall():
                        for orig_name in rule_names:
                            if orig_name.lower() == rule_name.lower():
                                usage_counts[orig_name] += count

    except Exception as e:
        logger.error(f"❌ Failed to query {env_prefix} DB for active rule references: {e}", exc_info=True)

    return usage_counts


def delete_rule_via_api(api_url: str, api_key: str, rule_name: str, deleted_by: str) -> bool:
    """Sends a DELETE request to soft-delete the specified rule via Microservice API."""
    endpoint = f"{api_url}/rules" if api_url.endswith("/mls-admin") else f"{api_url}/v1/mls-admin/rules"
    params = {"name": rule_name, "deleted_by": deleted_by}
    headers = {"accept": "application/json", "api-key": api_key}

    try:
        response = requests.delete(endpoint, headers=headers, params=params, timeout=15)
        if response.status_code in (200, 204):
            data = response.json() if response.text else {}
            rule_id = data.get("id", "N/A")
            logger.info(f"   ✅ Soft-deleted legacy rule '{rule_name}' (ID: {rule_id}) via API.")
            return True
        else:
            logger.error(
                f"   ❌ Soft-delete API failed for '{rule_name}' [HTTP {response.status_code}]: {response.text}")
            return False
    except requests.RequestException as e:
        logger.error(f"   ❌ Request exception for '{rule_name}': {e}", exc_info=True)
        return False


def execute_deletion_pass(env_prefix: str, rules_to_delete: list[str]) -> bool:
    """
    Executes API soft-deletions for the specified environment.
    Returns True ONLY if ALL candidate rules were successfully soft-deleted.
    """
    api_url = (os.getenv(f"MLS_ADMIN_{env_prefix}_URL") or os.getenv("MLS_ADMIN_STAGE_URL", "")).strip().strip(
        "'\"").rstrip("/")
    api_key = (os.getenv("MLS_ADMIN_API_KEY") or os.getenv("MS_API_KEY", "")).strip().strip("'\"")
    deleted_by_user = (os.getenv("DB_USER") or "migration_pipeline_bot").strip().strip("'\"")

    if not api_url or not api_key:
        logger.error(f"❌ Missing API configuration for {env_prefix} environment.")
        return False

    logger.info(
        f"🚀 Starting {env_prefix} Microservice API soft-deletion sequence for {len(rules_to_delete)} rule(s)...")
    success_count = 0
    for name in rules_to_delete:
        if delete_rule_via_api(api_url, api_key, name, deleted_by_user):
            success_count += 1

    logger.info(f"✅ [{env_prefix} PASS SUMMARY] Soft-deleted {success_count}/{len(rules_to_delete)} legacy rule(s).")
    return success_count == len(rules_to_delete) and len(rules_to_delete) > 0


def main() -> None:
    temp_data_dir = current_dir / "temp-data"

    logger.info("============================================================")
    logger.info("🗑️ STAGE 10: LEGACY RULE SOFT-DELETION ENGINE (GATED & INTERACTIVE)")
    logger.info("============================================================")

    legacy_candidates = load_legacy_candidates(temp_data_dir)
    if not legacy_candidates:
        logger.info("ℹ️ No legacy rules found in blueprint or safe_to_delete.csv. Nothing to delete.")
        return

    logger.info(f"📄 Loaded {len(legacy_candidates)} legacy candidate rule(s) for verification.")

    # 1. Step 1: Query Staging DB Dependencies
    logger.info("\n🔍 Step 1/2: Checking Staging DB active map dependencies...")
    stage_counts = get_active_rule_usage_counts_db("STAGE", legacy_candidates)

    # 2. Step 2: Query Production DB Dependencies
    logger.info("\n🔍 Step 2/2: Checking Production DB active map dependencies...")
    prod_counts = get_active_rule_usage_counts_db("PROD", legacy_candidates)

    rules_to_delete = []
    rules_retained = []

    for name in legacy_candidates:
        s_refs = stage_counts.get(name, 0)
        p_refs = prod_counts.get(name, 0)

        if s_refs > 0 or p_refs > 0:
            rules_retained.append((name, s_refs, p_refs))
        else:
            rules_to_delete.append(name)

    if rules_retained:
        logger.info(
            f"\n⚠️ Retaining {len(rules_retained)} rule(s) because they are still referenced by active UI maps:")
        for name, s_refs, p_refs in rules_retained:
            logger.info(f"   • '{name}' -> Active References: Stage={s_refs}, Prod={p_refs} (SKIPPED)")

    if not rules_to_delete:
        logger.info("\n🎉 No legacy rules are ready for soft-deletion (all candidates are still in active use).")
        return

    logger.info(f"\n✅ Verified {len(rules_to_delete)} fully unlinked legacy rule(s) ready for soft-deletion:")
    for idx, name in enumerate(rules_to_delete, 1):
        logger.info(f"   {idx}. {name}")

    # --- INTERACTIVE PROMPT 1: STAGE ---
    print("\n" + "=" * 60)
    stage_confirm = input(
        f"Do you want to soft-delete these {len(rules_to_delete)} legacy rule(s) on STAGE? (yes/no): ").strip().lower()

    stage_success = False
    if stage_confirm in ["yes", "y"]:
        stage_success = execute_deletion_pass("STAGE", rules_to_delete)
    else:
        logger.info("⏩ [STAGE] Soft-deletion skipped by user.")

    # --- STRICT PROD GATE: ONLY PROMPT IF STAGE SUCCEEDED ---
    print("\n" + "=" * 60)
    if not stage_success:
        logger.warning(
            "⛔ [PROD GATE BLOCK] Skipping Production soft-deletion because Stage soft-deletion was skipped or failed.")
        logger.info("============================================================")
        return

    prod_confirm = input(
        f"Stage soft-deletion completed successfully. Do you want to soft-delete these {len(rules_to_delete)} legacy rule(s) on PROD? (yes/no): ").strip().lower()
    if prod_confirm in ["yes", "y"]:
        execute_deletion_pass("PROD", rules_to_delete)
    else:
        logger.info("⏩ [PROD] Soft-deletion skipped by user.")

    logger.info("\n============================================================")
    logger.info("✅ STAGE 10 INTERACTIVE SOFT-DELETION SEQUENCE FINISHED")
    logger.info("============================================================")


if __name__ == "__main__":
    try:
        main()
    except (KeyboardInterrupt, SystemExit):
        force_terminal_exit()