"""
Draft Slack Notification Engine.

Reads target numeric MLS IDs from 'temp-data/promotion_sources.txt', fetches metadata
from the Staging database via SSH tunnel, maps protocol names from batch CSVs,
formats a GitHub-flavored Markdown table for Slack, and automatically copies the payload
to the system clipboard.
"""

import os
import io
import sys
import signal
import warnings
import subprocess
import psycopg2
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv

# Suppress Paramiko CryptographyDeprecationWarning logs prior to import
warnings.filterwarnings("ignore")

import paramiko
from tabulate import tabulate
from sshtunnel import SSHTunnelForwarder
from cryptography.hazmat.primitives import serialization
from pipeline_logger import setup_logger

# --- OS-LEVEL INSTANT TERMINAL EXIT ---
def force_terminal_exit(sig=None, frame=None):
    print("\n⛔ [TERMINAL ABORT] Killing process tree immediately...")
    os._exit(1)

signal.signal(signal.SIGINT, force_terminal_exit)
if hasattr(signal, "SIGBREAK"):
    signal.signal(signal.SIGBREAK, force_terminal_exit)

if not hasattr(paramiko, 'DSSKey'):
    paramiko.DSSKey = paramiko.PKey

current_dir = Path(__file__).resolve().parent
load_dotenv(dotenv_path=current_dir / ".env")

logger = setup_logger("Stage7_BulkPromotion")


def copy_to_clipboard(text: str) -> bool:
    """Cross-platform clipboard copy helper."""
    try:
        if sys.platform == "win32":
            process = subprocess.Popen("clip", stdin=subprocess.PIPE, close_fds=True)
            process.communicate(input=text.encode("utf-8"))
            return True
        elif sys.platform == "darwin":
            process = subprocess.Popen("pbcopy", stdin=subprocess.PIPE, close_fds=True)
            process.communicate(input=text.encode("utf-8"))
            return True
        else:
            # Linux fallback
            for cmd in ["xclip -selection clipboard", "xsel -b"]:
                try:
                    process = subprocess.Popen(cmd.split(), stdin=subprocess.PIPE, close_fds=True)
                    process.communicate(input=text.encode("utf-8"))
                    return True
                except FileNotFoundError:
                    continue
    except Exception as e:
        logger.warning(f"⚠️ Could not copy to system clipboard: {e}")
    return False


def get_db_connection_params():
    """Reads database credentials and SSH configuration from .env."""
    return {
        "db_name": os.getenv("DB_NAME", "mls_admin").strip().strip("'\""),
        "db_user": os.getenv("DB_USER", "").strip().strip("'\""),
        "db_password": os.getenv("DB_PASSWORD", "").strip().strip("'\""),
        "stage_db_host": (os.getenv("STAGE_DB_HOST") or os.getenv("DB_HOST", "")).strip().strip("'\""),
        "ssh_host": (os.getenv("STAGE_SSH_HOST") or os.getenv("SSH_HOST", "")).strip().strip("'\""),
        "ssh_user": os.getenv("SSH_USER", "").strip().strip("'\""),
        "ssh_key_path": os.getenv("SSH_KEY_PATH", "").strip().strip("'\""),
        "ssh_passphrase": os.getenv("SSH_KEY_PASSPHRASE", "").strip().strip("'\"")
    }


def fetch_metadata_from_db(kw_ids: list[int]) -> pd.DataFrame:
    """Executes SQL query against Staging DB strictly for requested promotion IDs."""
    if not kw_ids:
        return pd.DataFrame()

    config = get_db_connection_params()

    if not config["ssh_key_path"] or not os.path.exists(config["ssh_key_path"]):
        logger.warning(f"⚠️ Invalid or missing SSH key path: '{config['ssh_key_path']}'. Skipping DB query.")
        return pd.DataFrame()

    formatted_ids = ",".join(str(i) for i in kw_ids)
    sql_query = f"""
        SELECT m.id AS kw_id, m.id_str AS mls_id, v.name AS provider, m.sa_mls_id AS sa_id
        FROM public.mls m
        JOIN public.vendor v ON m.vendor_id = v.id
        WHERE m.id IN ({formatted_ids});
    """

    try:
        with open(config["ssh_key_path"], "rb") as key_file:
            private_key = serialization.load_pem_private_key(
                key_file.read(),
                password=config["ssh_passphrase"].encode() if config["ssh_passphrase"] else None,
            )
        pem_data = private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption(),
        )
        mypkey = paramiko.RSAKey.from_private_key(io.StringIO(pem_data.decode()))

        with SSHTunnelForwarder(
            (config["ssh_host"], 22),
            ssh_username=config["ssh_user"],
            ssh_pkey=mypkey,
            remote_bind_address=(config["stage_db_host"], 5432)
        ) as tunnel:
            conn = psycopg2.connect(
                dbname=config["db_name"],
                user=config["db_user"],
                password=config["db_password"],
                host="127.0.0.1",
                port=tunnel.local_bind_port
            )
            df = pd.read_sql_query(sql_query, conn)
            conn.close()
            return df

    except Exception as e:
        logger.warning(f"⚠️ SSH/Database metadata query failed: {e}")
        return pd.DataFrame()


def draft_slack_message() -> str:
    temp_data_dir = current_dir / "temp-data"
    manifest_path = temp_data_dir / "promotion_sources.txt"
    batch_files = [
        temp_data_dir / "batch_mls_targets.csv",
        temp_data_dir / "batch_mls_targets_api.csv",
        temp_data_dir / "batch_mls_targets_rets.csv"
    ]

    # 1. Read promoted MLS IDs from promotion_sources.txt
    if not manifest_path.exists():
        logger.info(f"ℹ️ Promotion manifest '{manifest_path.name}' not found. Skipping Slack draft generation.")
        return ""

    promoted_ids = [line.strip() for line in manifest_path.read_text(encoding="utf-8").splitlines() if line.strip()]

    if not promoted_ids:
        logger.info("ℹ️ No MLS IDs found in promotion manifest. Skipping Slack draft generation.")
        return ""

    promoted_ids_int = [int(x) for x in promoted_ids if x.isdigit()]

    # 2. Query DB metadata for promoted MLS IDs
    db_df = fetch_metadata_from_db(promoted_ids_int)

    if db_df.empty:
        logger.warning("⚠️ Could not fetch DB metadata for promotion IDs.")
        return ""

    # 3. Map protocol using batch file targets
    protocol_map = {}
    possible_proto_cols = ["download_protocol", "mls_protocol", "variant", "protocol", "type"]

    for batch_path in batch_files:
        if batch_path.exists():
            try:
                b_df = pd.read_csv(batch_path)
                proto_col = next((col for col in possible_proto_cols if col in b_df.columns), None)

                if proto_col:
                    id_col = "mls_id" if "mls_id" in b_df.columns else ("kw_id" if "kw_id" in b_df.columns else None)
                    if id_col:
                        for _, row in b_df.iterrows():
                            if pd.notna(row[id_col]) and int(row[id_col]) in promoted_ids_int:
                                protocol_map[int(row[id_col])] = str(row[proto_col])
            except Exception:
                pass

    # 4. Attach mapped protocol
    db_df["protocol"] = db_df["kw_id"].map(protocol_map).fillna("N/A")

    display_cols = ["kw_id", "mls_id", "provider", "sa_id", "protocol"]
    table_df = db_df[[col for col in display_cols if col in db_df.columns]]

    # 5. Format using GitHub Markdown style for native Slack table rendering
    ascii_table = tabulate(table_df, headers="keys", tablefmt="github", showindex=False)
    ticket_placeholder_url = "https://kwri.atlassian.net/browse/DMAP-1234"

    message_text = f"@here Promotion. Changes as per ticket {ticket_placeholder_url}\n\n{ascii_table}"

    logger.info("============================================================")
    logger.info("📢 DRAFT SLACK PROMOTION MESSAGE")
    logger.info("============================================================")
    print("\n" + message_text + "\n")
    logger.info("============================================================")

    # 6. Copy to System Clipboard
    if copy_to_clipboard(message_text):
        logger.info("📋 Draft message copied to clipboard! Remember to swap DMAP-1234 with your actual ticket before sending.")

    return message_text


if __name__ == "__main__":
    try:
        draft_slack_message()
    except (KeyboardInterrupt, SystemExit):
        force_terminal_exit()