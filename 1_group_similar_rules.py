"""
Groups and Compares Database Rules by Logic Equivalency with Ledger Locking & File Exclusion Rules.
Equipped with OS-level instant terminal abort signal handlers (SIGINT / SIGBREAK).
"""
import os
import io
import sys
import csv
import signal
import argparse
import logging
import warnings
import ast
import re
from pathlib import Path
from difflib import SequenceMatcher
from itertools import combinations
from datetime import datetime


# --- OS-LEVEL INSTANT TERMINAL EXIT ---
def force_terminal_exit(sig, frame):
    print("\n⛔ [TERMINAL ABORT] Killing process tree immediately...")
    os._exit(1)


signal.signal(signal.SIGINT, force_terminal_exit)
if hasattr(signal, "SIGBREAK"):
    signal.signal(signal.SIGBREAK, force_terminal_exit)

warnings.filterwarnings("ignore")
try:
    from cryptography.utils import CryptographyDeprecationWarning

    warnings.filterwarnings("ignore", category=CryptographyDeprecationWarning)
except ImportError:
    pass

import psycopg2
import paramiko
from dotenv import load_dotenv
from sshtunnel import SSHTunnelForwarder
from cryptography.hazmat.primitives import serialization
from Levenshtein import ratio

if not hasattr(paramiko, 'DSSKey'):
    paramiko.DSSKey = paramiko.PKey


def setup_logger():
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    today_str = datetime.now().strftime("%Y-%m-%d")
    log_file = log_dir / f"pipeline_{today_str}.log"

    logger = logging.getLogger("RuleGrouperAndComparer")
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


def load_excluded_patterns(logger: logging.Logger) -> list[str]:
    """Reads excluded rule keywords from excluded_rule_patterns.txt."""
    patterns = []
    file_paths = [Path("excluded_rule_patterns.txt"), Path("temp-data/excluded_rule_patterns.txt")]

    for path in file_paths:
        if path.exists():
            for line in path.read_text(encoding="utf-8").splitlines():
                cleaned = line.split("#")[0].strip()
                if cleaned:
                    patterns.append(cleaned.lower())
            logger.info(f"📋 Loaded {len(patterns)} exclusion pattern(s) from '{path}'.")
            return patterns

    logger.info("ℹ️ No 'excluded_rule_patterns.txt' found. Proceeding without pattern exclusions.")
    return patterns


def ensure_ledger_table_exists(cur, logger):
    create_table_sql = """
        CREATE TABLE IF NOT EXISTS public.processed_rules_ledger (
            rule_name VARCHAR(255) PRIMARY KEY,
            group_folder VARCHAR(255) NOT NULL,
            status VARCHAR(50) NOT NULL DEFAULT 'IN_PROGRESS',
            surviving_rule_name VARCHAR(255),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            created_by VARCHAR(100) NOT NULL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_by VARCHAR(100)
        );
    """
    cur.execute(create_table_sql)


def get_unprocessed_rules_from_db(logger):
    load_dotenv()
    ssh_host = (os.getenv("STAGE_SSH_HOST") or os.getenv("REMOTE_SSH_HOST") or os.getenv("SSH_HOST", "")).strip("'\"")
    ssh_user = (os.getenv("SSH_USER") or os.getenv("REMOTE_SSH_USER", "")).strip("'\"")
    ssh_key_path = (os.getenv("SSH_KEY_PATH") or os.getenv("REMOTE_SSH_KEY_PATH", "")).strip("'\"")
    ssh_passphrase = (os.getenv("SSH_KEY_PASSPHRASE") or os.getenv("REMOTE_SSH_KEY_PASSPHRASE", "")).strip("'\"")

    db_host = (os.getenv("STAGE_DB_HOST") or os.getenv("DB_HOST", "")).strip("'\"")
    db_name = (os.getenv("DB_NAME", "mls_admin")).strip("'\"")
    db_user = (os.getenv("DB_USER", "")).strip("'\"")
    db_pass = (os.getenv("DB_PASSWORD", "")).strip("'\"")

    if not ssh_host or not ssh_key_path:
        logger.error("❌ SSH parameters missing for database query.")
        sys.exit(1)

    excluded_patterns = load_excluded_patterns(logger)

    try:
        mypkey = load_ssh_pkey(ssh_key_path, ssh_passphrase)

        logger.info("Connecting via SSH tunnel to Staging database...")
        with SSHTunnelForwarder(
                (ssh_host, 22),
                ssh_username=ssh_user,
                ssh_pkey=mypkey,
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
                    ensure_ledger_table_exists(cur, logger)
                    conn.commit()

                    query = """
                        SELECT DISTINCT pr.id, pr.name, pr.content, pr.created_at, pr.created_by
                        FROM public.process_rule pr
                        JOIN public.map_rule_association mra ON pr.id = mra.process_rule_id
                        WHERE pr.deleted_at IS NULL
                          AND pr.name NOT IN (
                              SELECT rule_name FROM public.processed_rules_ledger
                              WHERE status IN ('IN_PROGRESS', 'COMPLETED', 'SKIPPED_NO_IDENTICAL')
                          );
                    """
                    cur.execute(query)
                    all_rules = cur.fetchall()

                    filtered_rules = []
                    skipped_count = 0
                    for rule in all_rules:
                        r_name_lower = rule[1].lower()
                        if any(pattern in r_name_lower for pattern in excluded_patterns):
                            skipped_count += 1
                            continue
                        filtered_rules.append(rule)

                    logger.info(
                        f"Successfully fetched {len(filtered_rules)} active rules ({skipped_count} excluded by pattern file).")
                    return filtered_rules, db_user

    except Exception as e:
        logger.error(f"❌ Failed to query database: {e}", exc_info=True)
        sys.exit(1)


def lock_rules_in_ledger(rule_group_map: dict, db_user: str, logger):
    if not rule_group_map:
        return

    load_dotenv()
    ssh_host = (os.getenv("STAGE_SSH_HOST") or os.getenv("REMOTE_SSH_HOST") or os.getenv("SSH_HOST", "")).strip("'\"")
    ssh_user = (os.getenv("SSH_USER") or os.getenv("REMOTE_SSH_USER", "")).strip("'\"")
    ssh_key_path = (os.getenv("SSH_KEY_PATH") or os.getenv("REMOTE_SSH_KEY_PATH", "")).strip("'\"")
    ssh_passphrase = (os.getenv("SSH_KEY_PASSPHRASE") or os.getenv("REMOTE_SSH_KEY_PASSPHRASE", "")).strip("'\"")

    db_host = (os.getenv("STAGE_DB_HOST") or os.getenv("DB_HOST", "")).strip("'\"")
    db_name = (os.getenv("DB_NAME", "mls_admin")).strip("'\"")
    db_pass = (os.getenv("DB_PASSWORD", "")).strip("'\"")

    try:
        mypkey = load_ssh_pkey(ssh_key_path, ssh_passphrase)

        with SSHTunnelForwarder(
                (ssh_host, 22),
                ssh_username=ssh_user,
                ssh_pkey=mypkey,
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
                    upsert_sql = """
                        INSERT INTO public.processed_rules_ledger (rule_name, group_folder, status, created_by, updated_by)
                        VALUES (%s, %s, 'IN_PROGRESS', %s, %s)
                        ON CONFLICT (rule_name) DO UPDATE 
                        SET status = EXCLUDED.status, updated_at = CURRENT_TIMESTAMP, updated_by = EXCLUDED.updated_by;
                    """
                    records = [(r_name, folder, db_user, db_user) for r_name, folder in rule_group_map.items()]
                    cur.executemany(upsert_sql, records)
                    conn.commit()
                    logger.info(f"🔒 Locked {len(records)} rules as 'IN_PROGRESS' in public.processed_rules_ledger.")
    except Exception as e:
        logger.error(f"❌ Failed to lock rules in DB ledger: {e}", exc_info=True)


class SkeletonMapper(ast.NodeTransformer):
    def visit_Name(self, n): return ast.copy_location(ast.Name('v', n.ctx), n)

    def visit_Constant(self, n): return ast.copy_location(ast.Constant('c'), n)

    def visit_FunctionDef(self, n): n.name = 'f'; self.generic_visit(n); return n

    def visit_AsyncFunctionDef(self, n): n.name = 'f'; self.generic_visit(n); return n


def get_normalized_structure(source):
    try:
        tree = ast.parse(source)
        for node in ast.walk(tree):
            if isinstance(node, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)) \
                    and node.body and isinstance(node.body[0], ast.Expr) \
                    and isinstance(node.body[0].value, ast.Constant):
                node.body.pop(0)
        return "".join(ast.unparse(SkeletonMapper().visit(tree)).split())
    except (SyntaxError, TypeError):
        return None


def run_grouping_and_comparison(limit: int, name_threshold: float, code_threshold: float):
    logger = setup_logger()
    logger.info("--- Starting Rule Grouping and Comparison Process ---")

    temp_dir = Path("temp-data")
    temp_dir.mkdir(parents=True, exist_ok=True)

    all_data, current_db_user = get_unprocessed_rules_from_db(logger)
    if not all_data:
        logger.info("No unprocessed active rules found.")
        return

    logger.info(f"Finding rule groups with name similarity >= {name_threshold:.0%}")
    rule_names = [rule[1] for rule in all_data]
    groups = []
    processed_indices = set()

    for i in range(len(rule_names)):
        if i in processed_indices:
            continue
        current_group = [rule_names[i]]
        processed_indices.add(i)
        for j in range(i + 1, len(rule_names)):
            if j in processed_indices:
                continue
            if ratio(rule_names[i], rule_names[j]) >= name_threshold:
                current_group.append(rule_names[j])
                processed_indices.add(j)
        if len(current_group) > 1:
            groups.append(sorted(current_group))

    if not groups:
        logger.info("No groups found matching the similarity threshold.")
        return

    logger.info(f"Found {len(groups)} total unanalysed groups. Processing top {limit} groups.")
    groups_to_process = groups[:limit]

    rule_metadata_map = {r[1]: (r[0], r[3], r[4]) for r in all_data}
    rule_content_map = {r[1]: r[2] for r in all_data}

    csv_rows = []
    created_group_dirs = []
    rule_to_folder_map = {}

    for i, group in enumerate(groups_to_process, 1):
        folder_name = min(group, key=len)
        group_dir = temp_dir / folder_name
        group_dir.mkdir(parents=True, exist_ok=True)
        created_group_dirs.append(group_dir)

        for rule_name in group:
            rule_to_folder_map[rule_name] = folder_name
            if rule_name in rule_metadata_map:
                meta = rule_metadata_map[rule_name]
                csv_rows.append((i, meta[0], rule_name, meta[1], meta[2]))

            file_path = group_dir / f"{rule_name}.py"
            file_path.write_text(rule_content_map.get(rule_name, "# Content unavailable"), encoding="utf-8")

    lock_rules_in_ledger(rule_to_folder_map, current_db_user, logger)

    rule_info_path = temp_dir / "rule_info.csv"
    with open(rule_info_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["group_id", "id", "name", "created_at", "created_by"])
        writer.writerows(sorted(csv_rows, key=lambda x: (x[0], x[2])))
    logger.info(f"Wrote rule metadata to '{rule_info_path}'.")

    comparison_rows = []
    for group_dir in created_group_dirs:
        files = list(group_dir.glob("*.py"))
        for file1, file2 in combinations(files, 2):
            struct1 = get_normalized_structure(file1.read_text(encoding="utf-8"))
            struct2 = get_normalized_structure(file2.read_text(encoding="utf-8"))

            if struct1 is None or struct2 is None:
                comparison_rows.append([group_dir.name, file1.name, file2.name, "Error", 0.0, "Syntax Error"])
                continue

            if struct1 == struct2:
                result, score = ("Identical", 1.0)
            else:
                score = round(SequenceMatcher(None, struct1, struct2).ratio(), 3)
                result = "Similar" if score >= code_threshold else "Different"

            comparison_rows.append([group_dir.name, file1.name, file2.name, result, score, ""])

    comparison_report_path = temp_dir / "comparison_report.csv"
    with open(comparison_report_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["group_folder", "file1", "file2", "comparison_result", "similarity_score", "notes"])
        writer.writerows(comparison_rows)

    logger.info(f"Wrote comparison results to '{comparison_report_path}'.")
    logger.info("--- Rule Grouping and Comparison Process Finished ---")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Groups and compares active rules from STAGING DB with DB Ledger Locking.")
    parser.add_argument("-l", "--limit", type=int, default=10, help="Max groups to process (Default: 10)")
    parser.add_argument("--name-threshold", type=float, default=0.75,
                        help="Rule name similarity threshold (Default: 0.75)")
    parser.add_argument("--code-threshold", type=float, default=0.85, help="Code similarity threshold (Default: 0.85)")
    args = parser.parse_args()

    run_grouping_and_comparison(args.limit, args.name_threshold, args.code_threshold)