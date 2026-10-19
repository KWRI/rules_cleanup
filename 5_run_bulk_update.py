"""
Pipeline Stage 5: Staging Workspace Operator (Configuration Bulk Modification Framework).

Executes bulk structural process map mutations across targeted database tables.
Supports dry-run validation simulation (--debug) and live mutations (--do-update).

Usage:
  # Dry-run inspection
  python 5_run_bulk_update.py --input-file temp-data/raw_targeted_rules.csv --debug

  # Live Staging DB update
  python 5_run_bulk_update.py --input-file temp-data/raw_targeted_rules.csv --do-update
"""

import sys
import os
import signal
import logging
import warnings
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv


# --- OS-LEVEL INSTANT TERMINAL EXIT ---
def force_terminal_exit(sig=None, frame=None):
    print("\n⛔ [TERMINAL ABORT] Killing process tree immediately...")
    os._exit(1)


signal.signal(signal.SIGINT, force_terminal_exit)
if hasattr(signal, "SIGBREAK"):
    signal.signal(signal.SIGBREAK, force_terminal_exit)

warnings.filterwarnings("ignore")

current_dir = Path(__file__).resolve().parent


def setup_logger():
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    today_str = datetime.now().strftime("%Y-%m-%d")
    log_file = log_dir / f"pipeline_{today_str}.log"

    logger = logging.getLogger("Stage5_BulkUpdate")
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

# 1. Load environment variables from .env first, then override from .env.bulk if present
standard_env_path = current_dir / ".env"
bulk_env_path = current_dir / ".env.bulk"

if standard_env_path.exists():
    load_dotenv(dotenv_path=standard_env_path, override=True)

if bulk_env_path.exists():
    load_dotenv(dotenv_path=bulk_env_path, override=True)
    logger.info("📋 Loaded configuration overrides from '.env.bulk'.")

# 2. Bridge DB and SSH Host aliases so bulk_map_tool finds expected keys
if os.getenv("STAGE_DB_HOST") and not os.getenv("DB_HOST"):
    os.environ["DB_HOST"] = os.getenv("STAGE_DB_HOST")

if os.getenv("STAGE_SSH_HOST") and not os.getenv("SSH_HOST"):
    os.environ["SSH_HOST"] = os.getenv("STAGE_SSH_HOST")

if os.getenv("MS_HOST") and not os.getenv("MLS_ADMIN_STAGE_URL"):
    os.environ["MLS_ADMIN_STAGE_URL"] = os.getenv("MS_HOST")

if os.getenv("MS_API_KEY") and not os.getenv("MLS_ADMIN_API_KEY"):
    os.environ["MLS_ADMIN_API_KEY"] = os.getenv("MS_API_KEY")

# 3. Path injections for external tool dependencies
tools_repo = os.getenv("TOOLS_REPO_PATH", "").strip().strip("'\"")
utils_repo = os.getenv("UTILS_REPO_PATH", "").strip().strip("'\"")
snowflake_repo = os.getenv("SNOWFLAKE_REPO_PATH", "").strip().strip("'\"")

if tools_repo and os.path.exists(tools_repo):
    sys.path.insert(0, os.path.join(tools_repo, "bulk_map_tool"))
    sys.path.insert(0, tools_repo)

if utils_repo and os.path.exists(utils_repo):
    sys.path.insert(0, utils_repo)

if snowflake_repo and os.path.exists(snowflake_repo):
    sys.path.insert(0, snowflake_repo)

# 4. Pydantic V2 to V1 Bridge
try:
    import pydantic
    import pydantic_settings


    class CleanSettings(pydantic_settings.BaseSettings):
        model_config = pydantic_settings.SettingsConfigDict(
            env_file=None,
            extra="ignore"
        )


    pydantic.BaseSettings = CleanSettings
    sys.modules['pydantic'].BaseSettings = CleanSettings
except Exception:
    pass


def run_stage_5():
    temp_data_dir = current_dir / "temp-data"
    raw_targeted_csv = temp_data_dir / "raw_targeted_rules.csv"

    if not raw_targeted_csv.exists() or raw_targeted_csv.stat().st_size == 0:
        logger.info(
            "ℹ️ No pending targeted rule mutations found in raw_targeted_rules.csv. Skipping Stage 5 DB mutation.")
        sys.exit(0)

    # Inspect if CSV contains data rows beyond header
    with open(raw_targeted_csv, "r", encoding="utf-8") as f:
        lines = [line.strip() for line in f if line.strip()]
        if len(lines) <= 1:
            logger.info("ℹ️ raw_targeted_rules.csv contains 0 data mutation rows. Skipping Stage 5 DB mutation.")
            sys.exit(0)

    try:
        import bulk_map_tool
    except ImportError as e:
        logger.error(f"❌ Missing required dependency 'bulk_map_tool': {e}")
        logger.error(f"Configured TOOLS_REPO_PATH: '{tools_repo}'")
        sys.exit(1)

    passthrough_args = sys.argv[1:]

    if passthrough_args:
        if "--input-file" in passthrough_args:
            idx = passthrough_args.index("--input-file") + 1
            if idx < len(passthrough_args):
                input_val = passthrough_args[idx]
                if not os.path.isabs(input_val) and "temp-data" not in input_val:
                    resolved_path = temp_data_dir / input_val
                    passthrough_args[idx] = str(resolved_path)

        logger.info(f"Handing off execution to bulk_map_tool with args: {' '.join(passthrough_args)}")
        sys.argv = ["bulk_map_tool.py"] + passthrough_args

        # Execute underlying bulk_map_tool main loop
        bulk_map_tool.main()
    else:
        logger.error("❌ No runtime arguments provided to Stage 5 bulk update operator.")
        logger.info(
            "Usage: python 5_run_bulk_update.py --input-file temp-data/raw_targeted_rules.csv [--debug | --do-update]")
        sys.exit(1)


if __name__ == "__main__":
    try:
        run_stage_5()
    except (KeyboardInterrupt, SystemExit):
        force_terminal_exit()