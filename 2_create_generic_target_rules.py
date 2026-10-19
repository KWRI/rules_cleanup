"""
Pre-processes identical rule clusters by deriving generic target rule names.
Performs CASE-INSENSITIVE DB checks and 100% AST logic identity verification.
If an existing generic rule name exists (regardless of ID vs Id capitalization) with DIFFERENT logic,
increments version suffixes (e.g., ListStatusIdV1, ListStatusIdV2) and registers the new versioned generic rule via API.

Outputs: temp-data/resolved_generic_targets.csv
"""
import os
import io
import sys
import csv
import re
import signal
import logging
import time
import requests
import warnings
import ast
from pathlib import Path
from collections import defaultdict


# --- OS-LEVEL INSTANT TERMINAL EXIT ---
def force_terminal_exit(sig=None, frame=None):
    print("\n⛔ [TERMINAL ABORT] Killing process tree immediately...")
    os._exit(1)


signal.signal(signal.SIGINT, force_terminal_exit)
if hasattr(signal, "SIGBREAK"):
    signal.signal(signal.SIGBREAK, force_terminal_exit)

warnings.filterwarnings("ignore")

import psycopg2
import paramiko
from dotenv import load_dotenv
from sshtunnel import SSHTunnelForwarder
from cryptography.hazmat.primitives import serialization

if not hasattr(paramiko, 'DSSKey'):
    paramiko.DSSKey = paramiko.PKey


def setup_logger():
    logger = logging.getLogger("GenericTargetRuleResolver")
    logger.setLevel(logging.INFO)
    if not logger.hasHandlers():
        formatter = logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    return logger


class SkeletonMapper(ast.NodeTransformer):
    def visit_Name(self, n): return ast.copy_location(ast.Name('v', n.ctx), n)

    def visit_Constant(self, n): return ast.copy_location(ast.Constant('c'), n)

    def visit_FunctionDef(self, n): n.name = 'f'; self.generic_visit(n); return n

    def visit_AsyncFunctionDef(self, n): n.name = 'f'; self.generic_visit(n); return n


def get_normalized_structure(source: str):
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


def are_ast_structures_identical(source1: str, source2: str) -> bool:
    if not source1 or not source2:
        return False
    s1 = get_normalized_structure(source1)
    s2 = get_normalized_structure(source2)
    return (s1 is not None) and (s1 == s2)


def to_pascal_case(name: str) -> str:
    base_name = name.removesuffix(".py")
    base_name = re.sub(r"[\(\)\[\]\{\}]", "", base_name)
    base_name = re.sub(r"[/|\\]|__", " ", base_name)
    # Fix trailing capital ID -> Id
    if base_name.endswith("ID"):
        base_name = base_name[:-2] + "Id"
    words = re.split(r"[\s_-]+", base_name)
    pascal_words = []
    for word in words:
        if not word:
            continue
        if re.search(r"[a-z][A-Z]", word) or re.search(r"\d+[A-Z]", word):
            pascal_words.append(word[0].upper() + word[1:])
        else:
            match = re.search(r"[a-zA-Z]", word)
            if match:
                idx = match.start()
                word = word[:idx] + word[idx].upper() + word[idx + 1:]
            pascal_words.append(word)
    pascal = "".join(pascal_words)
    return pascal if pascal else 'UnnamedRule'


def strip_numeric_prefix(name: str) -> str:
    """Strips leading numeric MLS prefixes and standardizes to PascalCase (e.g., '491ListStatusID' -> 'ListStatusId')."""
    base = name.removesuffix(".py")
    cleaned = re.sub(r"^\d+", "", base)
    return to_pascal_case(cleaned if cleaned else base)


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


def get_existing_db_rules_with_content(logger: logging.Logger) -> dict:
    """Returns mapping of lowercase_name -> (actual_db_name, content)."""
    ssh_host, ssh_user, ssh_key_path, ssh_passphrase, db_host, db_name, db_user, db_pass = get_db_connection_params()
    existing_rules = {}
    try:
        pkey = load_ssh_pkey(ssh_key_path, ssh_passphrase)
        logger.info("Connecting to DB to query existing active rule content...")
        with SSHTunnelForwarder((ssh_host, 22), ssh_username=ssh_user, ssh_pkey=pkey,
                                remote_bind_address=(db_host, 5432)) as tunnel:
            with psycopg2.connect(dbname=db_name, user=db_user, password=db_pass, host="127.0.0.1",
                                  port=tunnel.local_bind_port) as conn:
                with conn.cursor() as cur:
                    query = "SELECT name, content FROM public.process_rule WHERE deleted_at IS NULL;"
                    cur.execute(query)
                    for r_name, r_content in cur.fetchall():
                        # Key by lower case for case-insensitive lookup
                        existing_rules[r_name.lower()] = (r_name, r_content or "")
    except Exception as e:
        logger.error(f"Database error while fetching existing rules: {e}", exc_info=True)
    return existing_rules


def register_generic_rule_via_microservice(source_rule_name: str, generic_target_name: str,
                                           logger: logging.Logger) -> bool:
    load_dotenv()
    api_key = os.getenv("MLS_ADMIN_API_KEY", "").strip().strip("'\"")
    stage_url = os.getenv("MLS_ADMIN_STAGE_URL", "https://stage-ext-ms.data.kw.com/v1/mls-admin").strip().strip(
        "'\"").rstrip("/")
    rules_endpoint = f"{stage_url}/rules"

    if not api_key:
        logger.error("MLS_ADMIN_API_KEY missing from environment configuration.")
        return False

    ssh_host, ssh_user, ssh_key_path, ssh_passphrase, db_host, db_name, db_user, db_pass = get_db_connection_params()
    created_by_user = (os.getenv("CREATED_BY_USER") or os.getenv("UPDATED_BY_USER") or db_user).strip().strip("'\"")

    try:
        pkey = load_ssh_pkey(ssh_key_path, ssh_passphrase)
        with SSHTunnelForwarder((ssh_host, 22), ssh_username=ssh_user, ssh_pkey=pkey,
                                remote_bind_address=(db_host, 5432)) as tunnel:
            with psycopg2.connect(dbname=db_name, user=db_user, password=db_pass, host="127.0.0.1",
                                  port=tunnel.local_bind_port) as conn:
                with conn.cursor() as cur:
                    select_query = """
                        SELECT pr.content, pr.description, rl.name as language_name, rg.name as group_name
                        FROM public.process_rule pr
                        LEFT JOIN public.rule_language rl ON rl.id = pr.rule_language_id
                        LEFT JOIN public.rule_group_association rga ON rga.process_rule_id = pr.id
                        LEFT JOIN public.rule_group rg ON rg.id = rga.rule_group_id
                        WHERE pr.name = %s AND pr.deleted_at IS NULL;
                    """
                    cur.execute(select_query, (source_rule_name,))
                    source_record = cur.fetchone()

                    if not source_record:
                        logger.error(f"Source rule '{source_rule_name}' not found in DB.")
                        return False

                    content, description, lang_name, group_name = source_record
                    groups_payload = [group_name] if group_name else None

                    payload = {
                        "name": generic_target_name,
                        "content": content or "",
                        "created_by": created_by_user,
                        "language": lang_name or "Python",
                        "description": description or f"Standardized generic rule created from {source_rule_name}",
                        "params": {},
                        "groups": groups_payload,
                    }
                    headers = {"Content-Type": "application/json", "api-key": api_key}

                    for attempt in range(1, 4):
                        res = requests.post(rules_endpoint, json=payload, headers=headers, timeout=30)
                        if res.status_code in [200, 201]:
                            logger.info(
                                f"✨ Registered new generic rule '{generic_target_name}' via API | ID: {res.json().get('id')}")
                            return True
                        elif res.status_code == 409 or "already exists" in res.text.lower():
                            logger.info(f"ℹ️ Generic rule '{generic_target_name}' already exists in DB.")
                            return True
                        elif res.status_code in [502, 503, 504] and attempt < 3:
                            time.sleep(attempt * 2)
                            continue
                        else:
                            logger.error(f"Failed to register '{generic_target_name}' [{res.status_code}]: {res.text}")
                            return False
    except Exception as e:
        logger.error(f"Error registering generic rule '{generic_target_name}': {e}", exc_info=True)
        return False


def resolve_generic_targets():
    logger = setup_logger()
    logger.info("--- Starting Generic Target Rule Pre-processing ---")
    temp_dir = Path("temp-data")
    comparison_report_path = temp_dir / "comparison_report.csv"

    if not comparison_report_path.exists():
        logger.error(f"Missing {comparison_report_path}. Run group_similar_rules.py first.")
        return

    identical_groups = defaultdict(set)
    with open(comparison_report_path, 'r', encoding='utf-8') as f:
        for row in csv.DictReader(f):
            if row['comparison_result'] == 'Identical':
                rule1 = row['file1'].removesuffix(".py")
                rule2 = row['file2'].removesuffix(".py")
                identical_groups[row['group_folder']].update([rule1, rule2])

    if not identical_groups:
        logger.info("No 'Identical' rule clusters found in comparison report.")
        return

    existing_rules_lower = get_existing_db_rules_with_content(logger)
    resolved_mapping = {}

    for group_folder, rules in identical_groups.items():
        base_candidate = min(rules, key=len)
        candidate_file_path = temp_dir / group_folder / f"{base_candidate}.py"

        if candidate_file_path.exists():
            candidate_code = candidate_file_path.read_text(encoding="utf-8")
        else:
            _, candidate_code = existing_rules_lower.get(base_candidate.lower(), ("", ""))

        base_generic_name = strip_numeric_prefix(base_candidate)
        target_name = base_generic_name

        # Case-insensitive check in DB
        if target_name.lower() in existing_rules_lower:
            actual_db_name, db_code = existing_rules_lower[target_name.lower()]

            if are_ast_structures_identical(candidate_code, db_code):
                logger.info(
                    f"Group '{group_folder}' -> Generic rule '{actual_db_name}' exists with 100% MATCHING logic. Reusing '{actual_db_name}'.")
                target_name = actual_db_name
            else:
                # Logic differs! Force version increment ListStatusIdV1, ListStatusIdV2...
                version = 1
                versioned_target = f"{base_generic_name}V{version}"

                while versioned_target.lower() in existing_rules_lower:
                    actual_v_name, v_code = existing_rules_lower[versioned_target.lower()]
                    if are_ast_structures_identical(candidate_code, v_code):
                        versioned_target = actual_v_name
                        break
                    version += 1
                    versioned_target = f"{base_generic_name}V{version}"

                target_name = versioned_target

                if target_name.lower() not in existing_rules_lower:
                    logger.info(
                        f"Group '{group_folder}' -> Existing base generic rule code differed. Registering new versioned rule '{target_name}'...")
                    register_generic_rule_via_microservice(base_candidate, target_name, logger)
                    existing_rules_lower[target_name.lower()] = (target_name, candidate_code)
        else:
            # Base generic rule does not exist at all -> Register it via API
            logger.info(
                f"Group '{group_folder}' -> Base generic rule '{target_name}' missing from DB. Registering via API...")
            register_generic_rule_via_microservice(base_candidate, target_name, logger)
            existing_rules_lower[target_name.lower()] = (target_name, candidate_code)

        resolved_mapping[group_folder] = target_name

    out_csv = temp_dir / "resolved_generic_targets.csv"
    with open(out_csv, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(["group_folder", "resolved_generic_target"])
        for group_folder, generic_target in resolved_mapping.items():
            writer.writerow([group_folder, generic_target])

    logger.info(f"Generated '{out_csv}' with {len(resolved_mapping)} resolved generic rule targets.")
    logger.info("--- Generic Target Rule Pre-processing Finished ---")


if __name__ == "__main__":
    try:
        resolve_generic_targets()
    except (KeyboardInterrupt, SystemExit):
        force_terminal_exit()