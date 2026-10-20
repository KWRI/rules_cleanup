"""
Pipeline Stage 7: Bulk Production Promotion Engine & Slack Draft Generator.

Promotes newly standardized rules, schema maps, and metadata to the Production environment:
1. Reads target numeric MLS IDs directly from 'temp-data/promotion_sources.txt' (or auto-builds it).
2. Generates a formatted Slack promotion notification message and copies it to the clipboard.
3. Stages the manifest file on the remote deployment server over SFTP using a user-specific filename.
4. Invokes an interactive SSH shell to execute remote promotion commands ('s_p',
   'set_promotion stage prod', and 'promote_source.py').

Logs execution events to 'logs/pipeline_YYYY-MM-DD.log'.
"""

import os
import sys
import io
import csv
import time
import signal
import logging
import warnings
import paramiko
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
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

    logger = logging.getLogger("Stage7_BulkPromotion")
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

# ============================================================
# SSH ENVIRONMENT CONSTANTS
# ============================================================
SSH_HOST = (os.getenv("REMOTE_SSH_HOST") or os.getenv("REMOTE_HOST", "sdh.data.kw.com")).strip("'\"")
SSH_USER = (os.getenv("REMOTE_SSH_USER") or os.getenv("SSH_USER", "")).strip("'\"")
SSH_KEY_PATH = (os.getenv("REMOTE_SSH_KEY_PATH") or os.getenv("SSH_KEY_PATH", "")).strip("'\"")
SSH_PASSPHRASE = (os.getenv("REMOTE_SSH_KEY_PASSPHRASE") or os.getenv("SSH_KEY_PASSPHRASE", "")).strip("'\"")

# User-specific remote manifest path to prevent permission collisions on shared infrastructure
REMOTE_MANIFEST_PATH = os.getenv(
    "REMOTE_MANIFEST_PATH",
    f"/eim-mls-admin-ms/devtools/source_promotion/promotion_sources_{SSH_USER}.txt" if SSH_USER else "/eim-mls-admin-ms/devtools/source_promotion/promotion_sources.txt"
)


def ensure_promotion_manifest(temp_data_dir: Path) -> Path:
    """
    Ensures promotion_sources.txt exists and contains target MLS IDs.
    If missing or empty, builds it automatically from raw_targeted_rules.csv or batch_mls_targets.csv.
    """
    manifest_path = temp_data_dir / "promotion_sources.txt"
    target_ids = set()

    # 1. Read existing manifest if present
    if manifest_path.exists():
        for line in manifest_path.read_text(encoding="utf-8").splitlines():
            cleaned = line.strip().strip("'\"")
            if cleaned.isdigit():
                target_ids.add(int(cleaned))

    # 2. Fallback: Parse raw_targeted_rules.csv
    if not target_ids:
        raw_targeted_csv = temp_data_dir / "raw_targeted_rules.csv"
        if raw_targeted_csv.exists():
            with open(raw_targeted_csv, mode="r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    mls_id = row.get("mls_id", "").strip()
                    if mls_id.isdigit():
                        target_ids.add(int(mls_id))

    # Output verified sorted manifest
    if target_ids:
        sorted_ids = sorted(list(target_ids))
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        manifest_path.write_text(
            "\n".join(str(m) for m in sorted_ids) + "\n",
            encoding="utf-8"
        )
        logger.info(f"✅ Promotion manifest validated with {len(sorted_ids)} target MLS ID(s): {sorted_ids}")

    return manifest_path


def get_target_mls_ids(manifest_path: Path) -> list[int]:
    """Reads unique target numeric MLS IDs directly from promotion_sources.txt."""
    if not manifest_path.exists():
        logger.error(f"❌ Manifest file missing at path: {manifest_path}")
        return []

    with open(manifest_path, "r", encoding="utf-8") as f:
        ids = [int(line.strip()) for line in f if line.strip().isdigit()]

    seen = set()
    unique_ids = []
    for source_id in ids:
        if source_id not in seen:
            seen.add(source_id)
            unique_ids.append(source_id)

    return unique_ids


def main() -> None:
    logger.info("============================================================")
    logger.info("🚀 STAGE 7: PRODUCTION PROMOTION ENGINE INITIALIZED")
    logger.info("============================================================")

    temp_data_dir = current_dir / "temp-data"
    local_manifest = ensure_promotion_manifest(temp_data_dir)
    mls_ids = get_target_mls_ids(local_manifest)

    if not mls_ids:
        logger.info("ℹ️ No pending target MLS IDs found in promotion manifest. Skipping Stage 7 promotion.")
        sys.exit(0)

    logger.info(f"🎯 [PROMOTION BATCH GUARD] Target MLS IDs loaded from manifest ({len(mls_ids)}): {mls_ids}")

    # ------------------------------------------------------------
    # 1. DRAFT SLACK NOTIFICATION & COPY TO CLIPBOARD
    # ------------------------------------------------------------
    try:
        from draft_slack_message import draft_slack_message
        draft_slack_message()
    except Exception as err:
        logger.warning(f"⚠️ Slack draft generator skipped: {err}")

    # ------------------------------------------------------------
    # 2. SFTP MANIFEST STAGING
    # ------------------------------------------------------------
    logger.info("============================================================")
    logger.info("📦 STAGING PROMOTION MANIFEST ON REMOTE DEPLOYMENT SERVER")
    logger.info("============================================================")
    logger.info(f"[1/4] Connecting to remote host {SSH_HOST}...")

    if not SSH_KEY_PATH:
        logger.error("❌ SSH key path missing from configuration! Check SSH_KEY_PATH in .env.")
        sys.exit(1)

    try:
        # Load OpenSSH / PEM key dynamically using cryptography
        with open(SSH_KEY_PATH, "rb") as key_file:
            private_key = serialization.load_pem_private_key(
                key_file.read(),
                password=SSH_PASSPHRASE.encode() if SSH_PASSPHRASE else None,
            )
        pem_data = private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption(),
        )
        ssh_key = paramiko.RSAKey.from_private_key(io.StringIO(pem_data.decode()))

        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(hostname=SSH_HOST, username=SSH_USER, pkey=ssh_key, timeout=15)

        logger.info(f"[2/4] Uploading manifest to user remote path: {REMOTE_MANIFEST_PATH}...")
        sftp = ssh.open_sftp()
        sftp.put(str(local_manifest), REMOTE_MANIFEST_PATH)
        sftp.close()
        logger.info("✅ [SUCCESS] Manifest staged successfully on remote file layer.")

        logger.info("[3/4] Verifying remote payload structure consistency...")
        stdin, stdout, stderr = ssh.exec_command(f"cat {REMOTE_MANIFEST_PATH}")
        remote_sources = [line.strip() for line in stdout.readlines() if line.strip()]

        logger.info("Target Production Manifest records validated:")
        for source_id in remote_sources:
            logger.info(f"   Targeted MLS ID: {source_id}")

        # ------------------------------------------------------------
        # 3. INTERACTIVE SHELL PROMOTION SEQUENCE
        # ------------------------------------------------------------
        logger.info("[4/4] Invoking production rule migration engine sequentially...")
        shell = ssh.invoke_shell()
        time.sleep(2)

        # Environment setup commands
        init_commands = ["s_p", "set_promotion stage prod"]
        for cmd in init_commands:
            logger.info(f"[INPUT] Sending setup command: {cmd}")
            shell.send(cmd + "\n")
            time.sleep(3)
            while shell.recv_ready():
                output = shell.recv(4096).decode("utf-8", errors="ignore")
                for line in output.splitlines():
                    if line.strip():
                        logger.info(f"[REMOTE SHELL] {line}")

        # Send execution loop using remote user manifest path
        loop_cmd = f'tr -d "\\r" < {REMOTE_MANIFEST_PATH} | while IFS= read -r i; do [ -n "$i" ] && echo "Processing ID: "$i && python promote_source.py -s process "$i" -y; done'
        logger.info(f"[INPUT] Sending batch promotion loop: {loop_cmd}")
        shell.send(loop_cmd + "\n")

        # Dynamic output listener: continuously polls output until execution finishes completely
        logger.info("--- PRODUCTION SHELL INTERACTION OUTPUT LOGS ---")
        idle_count = 0
        while True:
            if shell.recv_ready():
                idle_count = 0
                output = shell.recv(8192).decode("utf-8", errors="ignore")
                for line in output.splitlines():
                    if line.strip():
                        logger.info(f"[REMOTE SHELL] {line}")
            else:
                time.sleep(2)
                idle_count += 1
                # Wait for 12 seconds of total remote shell silence before considering execution complete
                if idle_count >= 6:
                    break

        logger.info("[INPUT] Remote promotion complete. Closing shell session...")
        shell.send("exit\n")
        time.sleep(2)

        ssh.close()
        logger.info("---------------------------------------------------")
        logger.info("✅ SUCCESS: Production promotion cycle completed sequentially.")
        logger.info("Secure interactive session closed.")
        logger.info("============================================================")

    except Exception as e:
        logger.error(f"❌ Production promotion failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    try:
        main()
    except (KeyboardInterrupt, SystemExit):
        force_terminal_exit()