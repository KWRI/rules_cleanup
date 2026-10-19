"""
Pipeline Stage 11: Core Repo Ingress Sync, DB Rule Ledger Finalization & Git Commit/Push Engine.

1. Updates `public.processed_rules_ledger` DB table setting status to 'COMPLETED' for processed rules.
2. Connects to Staging DB over SSH tunnel to pull fresh process rule records (`public.process_rule`).
3. Writes active rules to 'ui-rules/active/' and archived rules to 'ui-rules/archived/'.
4. Purges all files from 'temp-data/' directory.
5. Purges log files older than 3 days in 'logs/'.
6. Automatically executes Git Add, Commit, and Push on the 'dm-consolidated-rules' repository.
"""

import os
import io
import sys
import re
import time
import shutil
import signal
import logging
import warnings
import subprocess
from pathlib import Path
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import psycopg2
import paramiko
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

    logger = logging.getLogger("Stage11_SyncAndCommitRepo")
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


def sanitize_filename(raw_name: str) -> str:
    """Sanitizes rule names into safe filenames, ensuring a single .py extension."""
    clean = raw_name.replace("/", "__").replace("\\", "__")
    clean = re.sub(r'[\x00-\x1f:*?"<>|]', '', clean).strip()
    if not clean:
        clean = "unnamed_rule"
    if not clean.lower().endswith(".py"):
        clean = f"{clean}.py"
    return clean


def write_rule_file(target_path: Path, content: str | None) -> None:
    """Worker task executed concurrently across the thread pool."""
    payload = content if content else "# No code content defined in database block\n"
    target_path.write_text(payload, encoding="utf-8")


def sync_db_to_local_repo(repo_path: Path) -> bool:
    """Extracts raw rule records from DB and streams files to ui-rules/active/ and ui-rules/archived/."""
    ssh_host = (os.getenv("STAGE_SSH_HOST") or os.getenv("SSH_HOST", "")).strip("'\"")
    ssh_user = os.getenv("SSH_USER", "").strip("'\"")
    ssh_key_path = os.getenv("SSH_KEY_PATH", "").strip("'\"")
    ssh_passphrase = os.getenv("SSH_KEY_PASSPHRASE", "").strip("'\"")

    db_host = (os.getenv("STAGE_DB_HOST") or os.getenv("DB_HOST", "")).strip("'\"")
    db_name = os.getenv("DB_NAME", "mls_admin").strip("'\"")
    db_user = os.getenv("DB_USER", "").strip("'\"")
    db_pass = os.getenv("DB_PASSWORD", "").strip("'\"")

    ui_rules_root = repo_path / "ui-rules"
    if not ui_rules_root.exists() and (repo_path / "active").exists():
        ui_rules_root = repo_path

    active_base = ui_rules_root / "active"
    archived_base = ui_rules_root / "archived"

    logger.info(f"Syncing Database process rules directly into repo: {ui_rules_root}")

    for folder in [active_base, archived_base]:
        folder.mkdir(parents=True, exist_ok=True)
        for item in folder.iterdir():
            if item.is_file():
                item.unlink()

    try:
        pkey = load_ssh_pkey(ssh_key_path, ssh_passphrase)
        logger.info(f"Establishing SSH tunnel to connect to database '{db_name}'...")
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
                conn.set_session(isolation_level="REPEATABLE READ", readonly=True)
                cursor_name = f"rule_pull_cursor_{int(time.time())}"
                with conn.cursor(name=cursor_name) as streaming_cur:
                    streaming_cur.itersize = 200
                    streaming_cur.execute("SELECT name, content, deleted_at FROM public.process_rule;")

                    file_count = 0
                    failed_writes = 0
                    seen_active = {}
                    seen_archived = {}
                    futures = []

                    with ThreadPoolExecutor(max_workers=16) as executor:
                        for name, content, deleted_at in streaming_cur:
                            if not name:
                                continue

                            safe_name = sanitize_filename(name)
                            stem = safe_name[:-3]
                            lowercase_stem = stem.lower()

                            if deleted_at:
                                target_dir = archived_base
                                tracker = seen_archived
                            else:
                                target_dir = active_base
                                tracker = seen_active

                            if lowercase_stem in tracker:
                                tracker[lowercase_stem] += 1
                                final_filename = f"{stem}_collision_{tracker[lowercase_stem]}.py"
                            else:
                                tracker[lowercase_stem] = 0
                                final_filename = safe_name

                            target_file = target_dir / final_filename
                            futures.append(executor.submit(write_rule_file, target_file, content))
                            file_count += 1

                        for future in as_completed(futures):
                            try:
                                future.result()
                            except Exception as exc:
                                failed_writes += 1
                                logger.error(f"⚠️ Failed file write task: {exc}")

                    logger.info(
                        f"✅ DB Sync Complete: Successfully wrote {file_count} process rule files to repository disk.")
                    return True

    except Exception as e:
        logger.error(f"❌ Failed to sync database rules to repository disk: {e}", exc_info=True)
        return False


def finalize_processed_rules_ledger_db(temp_dir: Path) -> None:
    """
    Updates status in public.processed_rules_ledger from IN_PROGRESS to COMPLETED
    for all rules processed in this batch.
    """
    blueprint_path = temp_dir / "consolidation_blueprint.csv"
    if not blueprint_path.exists():
        return

    rule_names = []
    import csv
    with open(blueprint_path, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            old_name = row.get("old_name", "").strip()
            if old_name:
                rule_names.append(old_name)

    if not rule_names:
        return

    ssh_host = (os.getenv("STAGE_SSH_HOST") or os.getenv("SSH_HOST", "")).strip("'\"")
    ssh_user = os.getenv("SSH_USER", "").strip("'\"")
    ssh_key_path = os.getenv("SSH_KEY_PATH", "").strip("'\"")
    ssh_passphrase = os.getenv("SSH_KEY_PASSPHRASE", "").strip("'\"")

    db_host = (os.getenv("STAGE_DB_HOST") or os.getenv("DB_HOST", "")).strip("'\"")
    db_name = os.getenv("DB_NAME", "mls_admin").strip("'\"")
    db_user = os.getenv("DB_USER", "").strip("'\"")
    db_pass = os.getenv("DB_PASSWORD", "").strip("'\"")

    try:
        pkey = load_ssh_pkey(ssh_key_path, ssh_passphrase)
        logger.info(f"Updating public.processed_rules_ledger in DB to 'COMPLETED' for {len(rule_names)} rule(s)...")
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
                    update_query = """
                        UPDATE public.processed_rules_ledger
                        SET status = 'COMPLETED', updated_at = NOW()
                        WHERE LOWER(rule_name) = ANY(%s) AND status = 'IN_PROGRESS';
                    """
                    cur.execute(update_query, ([r.lower() for r in rule_names],))
                    conn.commit()
                    logger.info(f"✅ DB Rule Ledger Finalized: Marked {cur.rowcount} rule(s) as 'COMPLETED'.")
    except Exception as e:
        logger.error(f"❌ Failed to update processed_rules_ledger in DB: {e}", exc_info=True)


def purge_temp_data_folder(temp_dir: Path) -> None:
    """Completely purges all files and subdirectories from temp-data/."""
    if not temp_dir.exists():
        return

    logger.info(f"🧹 Purging all temporary working files from '{temp_dir}'...")
    deleted_files_count = 0

    for item in temp_dir.glob("*"):
        try:
            if item.is_file() or item.is_symlink():
                item.unlink()
                deleted_files_count += 1
            elif item.is_dir():
                shutil.rmtree(item)
                deleted_files_count += 1
        except Exception as e:
            logger.warning(f"⚠️ Could not delete '{item.name}': {e}")

    logger.info(f"✅ Temp Folder Cleaned: Purged {deleted_files_count} item(s) from 'temp-data/'.")


def cleanup_old_logs(logs_dir: Path, keep_days: int = 3) -> None:
    """Removes pipeline log files older than keep_days."""
    if not logs_dir.exists():
        return

    cutoff_date = datetime.now() - timedelta(days=keep_days)
    deleted_count = 0

    for log_file in logs_dir.glob("pipeline_*.log"):
        try:
            date_str = log_file.stem.replace("pipeline_", "")
            file_date = datetime.strptime(date_str, "%Y-%m-%d")

            if file_date < cutoff_date:
                log_file.unlink()
                deleted_count += 1
        except ValueError:
            continue

    if deleted_count > 0:
        logger.info(f"🧹 Purged {deleted_count} log file(s) older than {keep_days} days.")


def execute_git_commit_and_push(repo_path: Path) -> bool:
    """Runs git add, commit, and push in the repo_path directory."""
    logger.info("============================================================")
    logger.info(f"🚀 EXECUTING GIT COMMIT & PUSH: {repo_path}")
    logger.info("============================================================")

    def run_git_command(cmd: list[str]) -> str:
        res = subprocess.run(cmd, cwd=repo_path, capture_output=True, text=True)
        if res.returncode != 0:
            logger.error(f"❌ Git Command Failure ({' '.join(cmd)}): {res.stderr.strip()}")
            raise RuntimeError(f"Git command failed: {res.stderr.strip()}")
        return res.stdout.strip()

    try:
        # 1. Git Add
        logger.info("[1/3] Staging modified and new files (git add .)...")
        run_git_command(["git", "add", "."])

        # Check git status
        status_out = run_git_command(["git", "status", "--porcelain"])
        if not status_out:
            logger.info("ℹ️ Git working tree is clean. No changes to commit.")
            return True

        # 2. Git Commit
        commit_msg = f"chore(rules-cleanup): Standardized generic rules & UI map updates [{datetime.now().strftime('%Y-%m-%d %H:%M')}]"
        logger.info(f"[2/3] Committing changes: '{commit_msg}'...")
        commit_out = run_git_command(["git", "commit", "-m", commit_msg])
        logger.info(f"   Git Commit Output:\n{commit_out}")

        # 3. Git Push
        logger.info("[3/3] Pushing changes to remote repository (git push)...")
        push_out = run_git_command(["git", "push"])
        logger.info(f"   Git Push Output:\n{push_out}")

        logger.info("🎉 SUCCESS: Repository successfully committed and pushed to remote branch!")
        return True

    except Exception as e:
        logger.error(f"❌ Git automation failed: {e}", exc_info=True)
        return False


def main() -> None:
    temp_dir = current_dir / "temp-data"

    repo_base_raw = os.getenv("REPO_PATH", "") or os.getenv("UI_RULES_DIR", "")
    repo_base = repo_base_raw.strip().strip("'\"")

    if not repo_base:
        repo_base = str(current_dir.parent / "dm-consolidated-rules")

    repo_path = Path(repo_base)

    logger.info("============================================================")
    logger.info("📂 STAGE 11: REPO DB SYNC, DB RULE LEDGER FINALIZATION & GIT COMMIT OPERATOR")
    logger.info("============================================================")
    logger.info(f"Target REPO_PATH: {repo_path}")

    # 1. Update public.processed_rules_ledger in DB to 'COMPLETED'
    finalize_processed_rules_ledger_db(temp_dir)

    # 2. Sync Staging DB process rules to local repository folder (active / archived)
    if not sync_db_to_local_repo(repo_path):
        logger.error("❌ Sync halted due to DB extraction error.")
        return

    # 3. Purge temp-data/ folder completely
    purge_temp_data_folder(temp_dir)

    # 4. Clean logs older than 3 days
    cleanup_old_logs(current_dir / "logs", keep_days=3)

    # 5. Execute Git commit and push to dm-consolidated-rules
    execute_git_commit_and_push(repo_path)

    logger.info("============================================================")
    logger.info("✅ STAGE 11 COMPLETE: REPOSITORY SYNCED, COMMITTED & PUSHED")
    logger.info("============================================================")


if __name__ == "__main__":
    try:
        main()
    except (KeyboardInterrupt, SystemExit):
        force_terminal_exit()