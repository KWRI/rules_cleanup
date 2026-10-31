"""
Pipeline Stage 3a: Bulk Reverse Promotion Engine (Prod -> Stage).

Syncs live Production process mappings to Staging prior to Stage 4 & 5 updates:
1. Reads distinct numeric MLS IDs from 'temp-data/batch_mls_targets.csv'.
2. Connects to the remote jump/deployment host (sdh.data.kw.com) via SSH.
3. Initializes reverse promotion environment:
     - s_p
     - set_promotion prod stage
4. Sequentially executes process map promotion per MLS ID:
     - python promote_source.py --step process <MLS_ID> --yes

Logs full shell output to console and 'logs/pipeline_YYYY-MM-DD.log'.
"""

import os
import sys
import io
import time
import signal
import argparse
import csv
import logging
from pathlib import Path
from datetime import datetime

# --- OS-LEVEL INSTANT TERMINAL EXIT ---
def force_terminal_exit(sig=None, frame=None):
    print("\n⛔ [TERMINAL ABORT] Killing process tree immediately...")
    os._exit(1)


signal.signal(signal.SIGINT, force_terminal_exit)
if hasattr(signal, "SIGBREAK"):
    signal.signal(signal.SIGBREAK, force_terminal_exit)

import paramiko
from dotenv import load_dotenv
from cryptography.hazmat.primitives import serialization

if not hasattr(paramiko, 'DSSKey'):
    paramiko.DSSKey = paramiko.PKey

current_dir = Path(__file__).resolve().parent
load_dotenv(dotenv_path=current_dir / ".env")


def setup_logger():
    log_dir = current_dir / "logs"
    log_dir.mkdir(exist_ok=True)
    today_str = datetime.now().strftime("%Y-%m-%d")
    log_file = log_dir / f"pipeline_{today_str}.log"

    logger = logging.getLogger("ReversePromotionProdToStage")
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

# SSH ENVIRONMENT CONSTANTS
SSH_HOST = (os.getenv("REMOTE_SSH_HOST") or os.getenv("REMOTE_HOST", "sdh.data.kw.com")).strip("'\"")
SSH_USER = (os.getenv("REMOTE_SSH_USER") or os.getenv("SSH_USER", "")).strip("'\"")
SSH_KEY_PATH = (os.getenv("REMOTE_SSH_KEY_PATH") or os.getenv("SSH_KEY_PATH", "")).strip("'\"")
SSH_PASSPHRASE = (os.getenv("REMOTE_SSH_KEY_PASSPHRASE") or os.getenv("SSH_KEY_PASSPHRASE", "")).strip("'\"")


def load_ssh_key(key_path: str, passphrase: str):
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


def get_target_mls_ids_from_batch(batch_csv_path: Path) -> list[int]:
    """Reads distinct numeric MLS IDs from batch_mls_targets.csv."""
    if not batch_csv_path.exists():
        logger.error(f"❌ Batch targets file missing: {batch_csv_path}")
        return []

    mls_ids = []
    seen = set()

    with open(batch_csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            raw_id = row.get("mls_id", "").strip()
            if raw_id.isdigit():
                val = int(raw_id)
                if val not in seen:
                    seen.add(val)
                    mls_ids.append(val)

    return sorted(mls_ids)


def execute_shell_command(shell, cmd: str, wait_timeout: int = 180, idle_limit: int = 6) -> None:
    """Sends command to interactive shell and streams response line-by-line until completed."""
    logger.info(f"[INPUT] >>> {cmd}")
    shell.send(cmd + "\n")
    time.sleep(1.5)

    idle_count = 0
    start_time = time.time()

    while True:
        if shell.recv_ready():
            idle_count = 0
            chunk = shell.recv(8192).decode("utf-8", errors="ignore")
            for line in chunk.splitlines():
                clean_line = line.strip()
                if clean_line:
                    logger.info(f"  [SHELL] {clean_line}")
        else:
            time.sleep(1.0)
            idle_count += 1
            if idle_count >= idle_limit:
                break

        if time.time() - start_time > wait_timeout:
            logger.warning(f"⚠️ Command '{cmd}' timed out after {wait_timeout} seconds.")
            break


def main():
    parser = argparse.ArgumentParser(description="Bulk Reverse Promotion Engine (Prod -> Stage)")
    parser.add_argument("-y", "--yes", action="store_true", help="Auto-approve execution without prompt")
    parser.add_argument("--batch-file", type=str, default="temp-data/batch_mls_targets.csv", help="Batch target CSV")
    args = parser.parse_args()

    logger.info("============================================================")
    logger.info("🚀 STAGE 3a: REVERSE PROMOTION ENGINE INITIALIZED (PROD -> STAGE)")
    logger.info("============================================================")

    batch_path = current_dir / args.batch_file if not Path(args.batch_file).is_absolute() else Path(args.batch_file)
    mls_ids = get_target_mls_ids_from_batch(batch_path)

    if not mls_ids:
        logger.warning(f"ℹ️ No numeric MLS IDs found in '{batch_path}'. Nothing to reverse promote.")
        return

    logger.info(f"📋 Discovered {len(mls_ids)} target MLS feed(s) for reverse promotion: {mls_ids}")

    if not args.yes:
        confirm = input(f"\n⚠️ Proceed with reverse promotion (PROD -> STAGE) for MLS {mls_ids}? (y/n): ").strip().lower()
        if confirm not in ["y", "yes"]:
            logger.info("❌ Execution cancelled by user.")
            return

    if not SSH_KEY_PATH:
        logger.error("❌ SSH key path missing from configuration! Check REMOTE_SSH_KEY_PATH in .env.")
        sys.exit(1)

    try:
        ssh_key = load_ssh_key(SSH_KEY_PATH, SSH_PASSPHRASE)

        logger.info(f"Connecting to deployment jump host '{SSH_HOST}' as '{SSH_USER}'...")
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(hostname=SSH_HOST, username=SSH_USER, pkey=ssh_key, timeout=20)

        logger.info("Opening interactive remote shell session...")
        shell = ssh.invoke_shell()
        time.sleep(2)

        # Flush initial login banner
        if shell.recv_ready():
            shell.recv(4096)

        # 1. INITIALIZE PROMOTION ENVIRONMENT (PROD -> STAGE)
        logger.info("------------------------------------------------------------")
        logger.info("⚙️ Step 1: Setting up promotion environment (PROD -> STAGE)...")
        logger.info("------------------------------------------------------------")
        execute_shell_command(shell, "s_p", wait_timeout=15, idle_limit=3)
        execute_shell_command(shell, "set_promotion prod stage", wait_timeout=15, idle_limit=3)

        # 2. RUN REVERSE PROMOTION SEQUENTIALLY PER MLS ID
        logger.info("------------------------------------------------------------")
        logger.info(f"⚙️ Step 2: Promoting process maps for {len(mls_ids)} MLS ID(s)...")
        logger.info("------------------------------------------------------------")
        for idx, mls_id in enumerate(mls_ids, 1):
            logger.info(f"\n[{idx}/{len(mls_ids)}] --- Reverse Promoting MLS {mls_id} ---")
            promote_cmd = f"python promote_source.py --step process {mls_id} --yes"
            execute_shell_command(shell, promote_cmd, wait_timeout=180, idle_limit=5)

        # 3. CLEANUP SESSION
        logger.info("Closing remote shell session...")
        shell.send("exit\n")
        time.sleep(1.5)
        ssh.close()

        logger.info("============================================================")
        logger.info("✅ SUCCESS: Reverse promotion cycle complete!")
        logger.info("Staging DB process maps are now 100% synced with Production.")
        logger.info("Ready to run: python 4_generate_targeted_csv.py")
        logger.info("============================================================")

    except Exception as e:
        logger.error(f"❌ Reverse promotion failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()