"""
Pre-processes identical rule clusters by deriving generic target rule names.
Implements multi-tier candidate scoring:
  1. Priority 1: Already standardized generic rule (PascalCase / Phase 2)
  2. Priority 2: Most recently maintained rule (latest updated_at)
  3. Priority 3: Fallback to oldest created_at

Performs CASE-INSENSITIVE DB checks and 100% AST logic identity verification.
Detects soft-deleted Stage DB rules and forces a versioned suffix (e.g., V1, V2)
to avoid API 404 lookup crashes during downstream promotion.

Modes:
  python 2_create_generic_target_rules.py               # DEFAULT: Preview mode (Generates CSV preview ONLY)
  python 2_create_generic_target_rules.py --do-creation # EXECUTE: Creates new rules via API from reviewed CSV
"""

import os
import io
import sys
import csv
import re
import signal
import logging
import time
import argparse
import requests
import warnings
import ast
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


def is_already_standardized_generic(name: str) -> bool:
    """Returns True if rule name is already standardized (PascalCase, no numeric MLS prefix)."""
    base = name.removesuffix(".py")
    if re.match(r"^\d+", base):
        return False
    if base.endswith("ID"):
        return False
    return bool(re.match(r"^[A-Z][a-zA-Z0-9]*$", base))


def to_pascal_case(name: str) -> str:
    base_name = name.removesuffix(".py")
    base_name = re.sub(r"[\(\)\[\]\{\}]", "", base_name)
    base_name = re.sub(r"[/|\\]|__", " ", base_name)
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


def get_existing_db_rules_with_metadata(logger: logging.Logger) -> tuple[dict, set]:
    ssh_host, ssh_user, ssh_key_path, ssh_passphrase, db_host, db_name, db_user, db_pass = get_db_connection_params()
    existing_rules = {}
    deleted_rules_lower = set()
    try:
        pkey = load_ssh_pkey(ssh_key_path, ssh_passphrase)
        logger.info("Connecting to DB to query existing active and soft-deleted rules...")
        with SSHTunnelForwarder((ssh_host, 22), ssh_username=ssh_user, ssh_pkey=pkey,
                                remote_bind_address=(db_host, 5432)) as tunnel:
            with psycopg2.connect(dbname=db_name, user=db_user, password=db_pass, host="127.0.0.1",
                                  port=tunnel.local_bind_port) as conn:
                with conn.cursor() as cur:
                    query = """
                        SELECT name, content, COALESCE(updated_at, created_at) AS updated_at, created_at, deleted_at 
                        FROM public.process_rule;
                    """
                    cur.execute(query)
                    for r_name, r_content, r_updated, r_created, r_deleted in cur.fetchall():
                        r_name_lower = r_name.lower()
                        if r_deleted is not None:
                            deleted_rules_lower.add(r_name_lower)
                        else:
                            existing_rules[r_name_lower] = (r_name, r_content or "", r_updated, r_created)
    except Exception as e:
        logger.error(f"Database error while fetching existing rules: {e}", exc_info=True)
    return existing_rules, deleted_rules_lower


def select_best_candidate_rule(rules: set, existing_rules_lower: dict) -> tuple[str, str]:
    scored_candidates = []

    for rule_name in rules:
        meta = existing_rules_lower.get(rule_name.lower())
        is_std = is_already_standardized_generic(rule_name)

        if meta:
            updated_at = meta[2] or datetime.min
            created_at = meta[3] or datetime.max
        else:
            updated_at = datetime.min
            created_at = datetime.max

        created_ts = created_at.timestamp() if isinstance(created_at, datetime) else 0.0

        score = (
            1 if is_std else 0,
            updated_at,
            -created_ts
        )

        reason = "ALREADY_GENERIC" if is_std else "MOST_RECENTLY_UPDATED"
        scored_candidates.append((score, rule_name, reason, updated_at))

    scored_candidates.sort(key=lambda x: x[0], reverse=True)
    best_match = scored_candidates[0]
    return best_match[1], best_match[2]


def register_generic_rule_via_microservice(source_rule_name: str, generic_target_name: str,
                                           logger: logging.Logger) -> bool:
    load_dotenv()
    api_key = os.getenv("MLS_ADMIN_API_KEY", "").strip().strip("'\"")
    stage_url = os.getenv("MLS_ADMIN_STAGE_URL", "https://stage-ext-ms.data.kw.com/v1/mls-admin").strip().strip("'\"").rstrip("/")
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
                            logger.info(f"✨ Created new generic rule '{generic_target_name}' via API | ID: {res.json().get('id')}")
                            return True
                        elif res.status_code == 409 or "already exists" in res.text.lower():
                            logger.info(f"ℹ️ Generic rule '{generic_target_name}' already exists in DB.")
                            return True
                        elif res.status_code in [502, 503, 504] and attempt < 3:
                            time.sleep(attempt * 2)
                            continue
                        else:
                            logger.error(f"Failed to create '{generic_target_name}' [{res.status_code}]: {res.text}")
                            return False
    except Exception as e:
        logger.error(f"Error creating generic rule '{generic_target_name}': {e}", exc_info=True)
        return False


def resolve_generic_targets(do_creation: bool = False):
    logger = setup_logger()
    logger.info("============================================================")
    if do_creation:
        logger.info("🚀 STAGE 2: EXECUTION MODE (CREATING TARGET RULES VIA API)")
    else:
        logger.info("🔍 STAGE 2: PREVIEW MODE (GENERATING PREVIEW CSV - NO DB MUTATION)")
    logger.info("============================================================")

    temp_dir = Path("temp-data")
    comparison_report_path = temp_dir / "comparison_report.csv"
    out_csv = temp_dir / "resolved_generic_targets.csv"

    # --- EXECUTION MODE: Read reviewed CSV and create pending rules ---
    if do_creation:
        if not out_csv.exists():
            logger.error(f"❌ Missing '{out_csv}'. Run Stage 2 in preview mode first.")
            return

        created_count = 0
        with open(out_csv, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                action = row.get("action", "")
                src_cand = row.get("source_candidate", "")
                target_name = row.get("resolved_generic_target", "")

                if action == "CREATE_NEW" and src_cand and target_name:
                    logger.info(f"Creating generic rule '{target_name}' (source: '{src_cand}')...")
                    if register_generic_rule_via_microservice(src_cand, target_name, logger):
                        created_count += 1

        logger.info(f"✅ Executed rule creation pass: Successfully created {created_count} generic rule(s) via API.")
        return

    # --- PREVIEW MODE: Generate resolved_generic_targets.csv WITHOUT API Calls ---
    if not comparison_report_path.exists():
        logger.error(f"Missing {comparison_report_path}. Run 1_group_similar_rules.py first.")
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

    existing_rules_lower, deleted_rules_lower = get_existing_db_rules_with_metadata(logger)
    resolved_mapping = {}

    for group_folder, rules in identical_groups.items():
        selected_candidate, selection_reason = select_best_candidate_rule(rules, existing_rules_lower)

        candidate_file_path = temp_dir / group_folder / f"{selected_candidate}.py"
        if candidate_file_path.exists():
            candidate_code = candidate_file_path.read_text(encoding="utf-8")
        else:
            _, candidate_code, _, _ = existing_rules_lower.get(selected_candidate.lower(), ("", "", None, None))

        base_generic_name = strip_numeric_prefix(selected_candidate)
        target_name = base_generic_name
        action = "REUSE_EXISTING"

        # --- CHECK IF PROPOSED TARGET NAME IS SOFT-DELETED IN STAGE DB ---
        if target_name.lower() in deleted_rules_lower:
            version = 1
            versioned_target = f"{base_generic_name}V{version}"
            while versioned_target.lower() in existing_rules_lower or versioned_target.lower() in deleted_rules_lower:
                version += 1
                versioned_target = f"{base_generic_name}V{version}"

            target_name = versioned_target
            action = "CREATE_NEW"
            logger.info(
                f"Group '{group_folder}' -> Base generic rule '{base_generic_name}' is SOFT-DELETED in Stage DB. Proposed NEW versioned rule '{target_name}' (Action: {action})"
            )

        elif target_name.lower() in existing_rules_lower:
            actual_db_name, db_code, _, _ = existing_rules_lower[target_name.lower()]

            if are_ast_structures_identical(candidate_code, db_code):
                logger.info(
                    f"Group '{group_folder}' -> Selected candidate '{selected_candidate}' ({selection_reason}). Generic rule '{actual_db_name}' EXISTS with 100% MATCHING logic."
                )
                target_name = actual_db_name
                action = "REUSE_EXISTING"
            else:
                version = 1
                versioned_target = f"{base_generic_name}V{version}"

                while versioned_target.lower() in existing_rules_lower or versioned_target.lower() in deleted_rules_lower:
                    actual_v_data = existing_rules_lower.get(versioned_target.lower())
                    if actual_v_data:
                        actual_v_name, v_code, _, _ = actual_v_data
                        if v_code and are_ast_structures_identical(candidate_code, v_code):
                            versioned_target = actual_v_name
                            action = "REUSE_EXISTING"
                            break
                    version += 1
                    versioned_target = f"{base_generic_name}V{version}"

                target_name = versioned_target
                if target_name.lower() not in existing_rules_lower:
                    action = "CREATE_NEW"
                    logger.info(
                        f"Group '{group_folder}' -> Code differed. Proposed NEW versioned rule '{target_name}' (Action: {action})"
                    )
        else:
            action = "CREATE_NEW"
            logger.info(
                f"Group '{group_folder}' -> Proposed NEW base generic rule '{target_name}' (Action: {action})"
            )

        resolved_mapping[group_folder] = (target_name, selected_candidate, selection_reason, action)

    with open(out_csv, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(["group_folder", "resolved_generic_target", "source_candidate", "selection_reason", "action"])
        for group_folder, (generic_target, src_cand, reason, act) in resolved_mapping.items():
            writer.writerow([group_folder, generic_target, src_cand, reason, act])

    logger.info("============================================================")
    logger.info(f"📋 PREVIEW GENERATED: '{out_csv}' ({len(resolved_mapping)} targets).")
    logger.info("   Review the CSV file and candidate Python code files in 'temp-data/'.")
    logger.info("   When ready, execute creation with:")
    logger.info("   python 2_create_generic_target_rules.py --do-creation")
    logger.info("============================================================")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Stage 2 Generic Target Resolver with Preview Safety Gate")
    parser.add_argument("--do-creation", action="store_true", help="Executes API rule creation for pending rules")
    args = parser.parse_args()

    try:
        resolve_generic_targets(do_creation=args.do_creation)
    except (KeyboardInterrupt, SystemExit):
        force_terminal_exit()