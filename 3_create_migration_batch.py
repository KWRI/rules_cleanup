"""
Create Migration Batch for Identical Rules.
Uses resolved generic target rules to build consolidation manifests and target MLS feed CSVs.
"""
import warnings
warnings.filterwarnings("ignore")

import os
import io
import sys
import csv
import re
import signal
import logging
from pathlib import Path
from collections import defaultdict
from datetime import datetime

# --- OS-LEVEL INSTANT TERMINAL EXIT ---
def force_terminal_exit(sig=None, frame=None):
    print("\n⛔ [TERMINAL ABORT] Killing process tree immediately...")
    os._exit(1)

signal.signal(signal.SIGINT, force_terminal_exit)
if hasattr(signal, "SIGBREAK"):
    signal.signal(signal.SIGBREAK, force_terminal_exit)

import psycopg2
import psycopg2.extras
import paramiko
from dotenv import load_dotenv
from sshtunnel import SSHTunnelForwarder
from cryptography.hazmat.primitives import serialization

if not hasattr(paramiko, 'DSSKey'):
    paramiko.DSSKey = paramiko.PKey


def setup_logger():
    logger = logging.getLogger("MigrationBatchCreator")
    logger.setLevel(logging.INFO)
    if not logger.hasHandlers():
        formatter = logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    return logger


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


def get_db_connection_params():
    load_dotenv()
    ssh_host = (os.getenv("STAGE_SSH_HOST") or os.getenv("REMOTE_SSH_HOST") or os.getenv("SSH_HOST", "")).strip("'\"")
    ssh_user = (os.getenv("SSH_USER") or os.getenv("REMOTE_SSH_USER", "")).strip("'\"")
    ssh_key_path = (os.getenv("SSH_KEY_PATH") or os.getenv("REMOTE_SSH_KEY_PATH", "")).strip("'\"")
    ssh_passphrase = (os.getenv("SSH_KEY_PASSPHRASE") or os.getenv("REMOTE_SSH_KEY_PASSPHRASE", "")).strip("'\"")

    db_host = (os.getenv("STAGE_DB_HOST") or os.getenv("DB_HOST", "")).strip("'\"")
    db_name = (os.getenv("DB_NAME", "mls_admin")).strip("'\"")
    db_user = (os.getenv("DB_USER", "")).strip("'\"")
    db_pass = (os.getenv("DB_PASSWORD", "")).strip("'\"")
    return ssh_host, ssh_user, ssh_key_path, ssh_passphrase, db_host, db_name, db_user, db_pass


def get_mapping_counts(rule_names: list, logger: logging.Logger) -> dict:
    if not rule_names:
        return {}
    ssh_host, ssh_user, ssh_key_path, ssh_passphrase, db_host, db_name, db_user, db_pass = get_db_connection_params()
    counts = {name: 0 for name in rule_names}
    try:
        pkey = load_ssh_pkey(ssh_key_path, ssh_passphrase)
        logger.info(f"Fetching active mapping counts for {len(rule_names)} candidate rules...")
        with SSHTunnelForwarder((ssh_host, 22), ssh_username=ssh_user, ssh_pkey=pkey,
                                remote_bind_address=(db_host, 5432)) as tunnel:
            with psycopg2.connect(dbname=db_name, user=db_user, password=db_pass, host="127.0.0.1",
                                  port=tunnel.local_bind_port) as conn:
                with conn.cursor() as cur:
                    query = """
                        SELECT pr.name, COUNT(DISTINCT mra.process_map_id) 
                        FROM public.process_rule pr 
                        JOIN public.map_rule_association mra ON pr.id = mra.process_rule_id 
                        WHERE pr.name = ANY(%s) 
                          AND pr.deleted_at IS NULL
                        GROUP BY pr.name;
                    """
                    cur.execute(query, (rule_names,))
                    for row in cur.fetchall():
                        counts[row[0]] = int(row[1])
    except Exception as e:
        logger.error(f"Database error while getting mapping counts: {e}", exc_info=True)
    return counts


def get_mls_details_for_rules(rule_names: list, logger: logging.Logger) -> list:
    if not rule_names:
        return []
    ssh_host, ssh_user, ssh_key_path, ssh_passphrase, db_host, db_name, db_user, db_pass = get_db_connection_params()
    try:
        pkey = load_ssh_pkey(ssh_key_path, ssh_passphrase)
        logger.info("Fetching active MLS configurations for legacy duplicate rules...")
        with SSHTunnelForwarder((ssh_host, 22), ssh_username=ssh_user, ssh_pkey=pkey,
                                remote_bind_address=(db_host, 5432)) as tunnel:
            with psycopg2.connect(dbname=db_name, user=db_user, password=db_pass, host="127.0.0.1",
                                  port=tunnel.local_bind_port) as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                    query = """
                        SELECT DISTINCT 
                            pr.name AS legacy_rule_name, 
                            m.id AS mls_id, 
                            m.id_str AS mls_id_str, 
                            m.sa_mls_id, 
                            m.sa_mls_id_str, 
                            m.mls_status_id, 
                            dp.name AS download_protocol, 
                            ctm.content_type, 
                            ctm.content_sub_type 
                        FROM public.process_rule pr 
                        JOIN public.map_rule_association mra ON pr.id = mra.process_rule_id 
                        JOIN public.mls_resource mr ON mr.process_map_id = mra.process_map_id 
                        JOIN public.content_type_map ctm ON mr.content_type_map_id = ctm.id 
                        JOIN public.mls m ON mr.mls_id = m.id AND m.mls_status_id = 2 
                        JOIN public.download_mls dm ON m.id = dm.mls_id 
                        JOIN public.download_config dc ON dm.download_config_id = dc.id 
                        JOIN public.download_protocol dp ON dc.download_protocol_id = dp.id 
                        WHERE pr.name = ANY(%s)
                          AND pr.deleted_at IS NULL
                          AND m.id NOT IN (7, 30, 33, 34, 46, 86);
                    """
                    cur.execute(query, (rule_names,))
                    return [dict(row) for row in cur.fetchall()]
    except Exception as e:
        logger.error(f"Database error while getting MLS details: {e}", exc_info=True)
        return []


def create_batch():
    logger = setup_logger()
    logger.info("--- Starting Migration Batch Manifest Generation ---")
    temp_dir = Path("temp-data")
    comparison_report_path = temp_dir / "comparison_report.csv"
    generic_targets_path = temp_dir / "resolved_generic_targets.csv"

    if not all([comparison_report_path.exists(), generic_targets_path.exists()]):
        logger.error("Required pre-processing files missing in temp-data/. Run 2_create_generic_target_rules.py first.")
        return

    generic_targets_map = {row['group_folder']: row['resolved_generic_target']
                           for row in csv.DictReader(generic_targets_path.open(encoding='utf-8'))}

    identical_groups = defaultdict(set)
    with open(comparison_report_path, 'r', encoding='utf-8') as f:
        for row in csv.DictReader(f):
            if row['comparison_result'] == 'Identical':
                rule1 = row['file1'].removesuffix(".py")
                rule2 = row['file2'].removesuffix(".py")
                identical_groups[row['group_folder']].update([rule1, rule2])

    if not identical_groups:
        logger.info("No 'Identical' rules found across groups.")
        return

    consolidation_map = {}
    safe_to_delete_list = []

    all_rules = list({rule for group in identical_groups.values() for rule in group})
    mapping_counts = get_mapping_counts(all_rules, logger)

    for group_folder, rules in identical_groups.items():
        generic_target = generic_targets_map.get(group_folder)
        if not generic_target:
            continue

        for dup_rule in rules:
            if dup_rule == generic_target:
                continue

            count = mapping_counts.get(dup_rule, 0)
            if count == 0:
                safe_to_delete_list.append(dup_rule)
            else:
                consolidation_map[dup_rule] = generic_target

    blueprint_path = temp_dir / "consolidation_blueprint.csv"
    with open(blueprint_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(["old_name", "target_new_name"])
        for old, new in consolidation_map.items():
            writer.writerow([old, new])
    logger.info(f"Generated 'consolidation_blueprint.csv' with {len(consolidation_map)} active duplicate mappings.")

    batch_target_path = temp_dir / "batch_mls_targets.csv"
    header = ["mls_id", "mls_id_str", "sa_mls_id", "sa_mls_id_str", "mls_status_id", "download_protocol",
              "content_type", "content_sub_type", "legacy_rule_name"]
    with open(batch_target_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        if consolidation_map:
            mls_mappings = get_mls_details_for_rules(list(consolidation_map.keys()), logger)
            writer.writerows(mls_mappings)
            logger.info(f"Generated 'batch_mls_targets.csv' for {len(mls_mappings)} active mappings.")
        else:
            logger.info("Generated 'batch_mls_targets.csv' with 0 mappings.")

    safe_to_delete_path = temp_dir / "safe_to_delete.csv"
    with open(safe_to_delete_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(["rule_name_to_delete"])
        for rule_name in sorted(list(set(safe_to_delete_list))):
            writer.writerow([rule_name])
    logger.info(f"Generated 'safe_to_delete.csv' with {len(set(safe_to_delete_list))} unmapped rules.")

    logger.info("--- Migration Batch Creation Finished ---")


if __name__ == "__main__":
    try:
        create_batch()
    except (KeyboardInterrupt, SystemExit):
        force_terminal_exit()