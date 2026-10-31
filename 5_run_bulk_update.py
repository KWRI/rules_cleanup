"""
Pipeline Stage 5: Staging Workspace Operator (Process Map Bulk Modification Engine).

Executes bulk structural process map mutations across targeted database tables in Staging:
1. Locates mutation payload in 'temp-data/final_targeted_rules.csv'.
2. Inspects payload statistics (total rows, rule function targets, affected MLS feeds).
3. Supports batch limiting (--max-groups / --max-mls).
4. Supports dry-run validation simulation (--debug) and live mutations (--do-update).
5. Bridges environment aliases and invokes 'bulk_map_tool' to apply updates.

Logs execution events to console and 'logs/pipeline_YYYY-MM-DD.log'.
"""

import sys
import os
import csv
import signal
import logging
import argparse
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
    """Initializes console and rotating daily file logger."""
    log_dir = current_dir / "logs"
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

# 1. Load environment variables
standard_env_path = current_dir / ".env"
bulk_env_path = current_dir / ".env.bulk"

if standard_env_path.exists():
    load_dotenv(dotenv_path=standard_env_path, override=True)

if bulk_env_path.exists():
    load_dotenv(dotenv_path=bulk_env_path, override=True)
    logger.info("📋 Loaded configuration overrides from '.env.bulk'.")

# 2. Bridge DB and SSH Host aliases
if os.getenv("STAGE_DB_HOST") and not os.getenv("DB_HOST"):
    os.environ["DB_HOST"] = os.getenv("STAGE_DB_HOST")

if os.getenv("STAGE_SSH_HOST") and not os.getenv("SSH_HOST"):
    os.environ["SSH_HOST"] = os.getenv("STAGE_SSH_HOST")

if os.getenv("MS_HOST") and not os.getenv("MLS_ADMIN_STAGE_URL"):
    os.environ["MLS_ADMIN_STAGE_URL"] = os.getenv("MS_HOST")

if os.getenv("MS_API_KEY") and not os.getenv("MLS_ADMIN_API_KEY"):
    os.environ["MLS_ADMIN_API_KEY"] = os.getenv("MS_API_KEY")

# 3. External tool dependency paths
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

# 4. Pydantic settings bridge
try:
    import pydantic
    import pydantic_settings

    class CleanSettings(pydantic_settings.BaseSettings):
        model_config = pydantic_settings.SettingsConfigDict(env_file=None, extra="ignore")

    pydantic.BaseSettings = CleanSettings
    sys.modules["pydantic"].BaseSettings = CleanSettings
except Exception:
    pass


def inspect_and_filter_payload(input_file: Path, max_groups: int = None, max_mls: int = None) -> Path:
    """Reads input CSV, reports key stats, and slices payload if max_groups or max_mls is specified."""
    with open(input_file, mode="r", encoding="utf-8") as f:
        reader = list(csv.DictReader(f))

    if not reader:
        logger.info("ℹ️ Input CSV contains 0 data mutation rows.")
        sys.exit(0)

    # Gather stats
    unique_rules = set()
    unique_mls = set()
    for row in reader:
        rf = row.get("rule_func", "")
        if rf:
            unique_rules.add(rf)
        mls = row.get("mls_id", "")
        if mls:
            unique_mls.add(mls)

    logger.info("============================================================")
    logger.info("📊 STAGE 5 BULK UPDATE PAYLOAD INSPECTION:")
    logger.info(f"   Target Input CSV            : {input_file}")
    logger.info(f"   Total Configuration Rows   : {len(reader)}")
    logger.info(f"   Unique Rule Function Targets: {len(unique_rules)}")
    logger.info(f"   Unique Affected MLS Feeds   : {len(unique_mls)}")
    logger.info("============================================================")

    # Apply batch slicing if requested
    filtered_rows = reader
    if max_mls and max_mls > 0:
        allowed_mls = set(sorted(list(unique_mls))[:max_mls])
        filtered_rows = [r for r in filtered_rows if r.get("mls_id") in allowed_mls]
        logger.info(
            f"✂️ Batch Limiter Applied: Filtered payload to first {len(allowed_mls)} MLS IDs ({len(filtered_rows)} rows)."
        )

    if max_groups and max_groups > 0:
        allowed_rules = set(sorted(list(unique_rules))[:max_groups])
        filtered_rows = [r for r in filtered_rows if r.get("rule_func") in allowed_rules]
        logger.info(
            f"✂️ Batch Limiter Applied: Filtered payload to first {len(allowed_rules)} Rule Groups ({len(filtered_rows)} rows)."
        )

    # If batch slicing occurred, write temporary batched CSV
    if len(filtered_rows) < len(reader):
        batched_csv_path = input_file.parent / "batched_targeted_rules.csv"
        fieldnames = list(reader[0].keys())
        with open(batched_csv_path, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(filtered_rows)
        logger.info(f"💾 Created batch execution payload: {batched_csv_path}")
        return batched_csv_path

    return input_file


def run_stage_5():
    """Main Stage 5 orchestrator."""
    temp_data_dir = current_dir / "temp-data"

    # Locate candidate input file
    input_file_candidate = temp_data_dir / "final_targeted_rules.csv"
    if not input_file_candidate.exists():
        input_file_candidate = temp_data_dir / "raw_targeted_rules.csv"

    if not input_file_candidate.exists() or input_file_candidate.stat().st_size == 0:
        logger.info("ℹ️ No pending targeted rule mutations found in temp-data/. Skipping Stage 5 DB mutation.")
        sys.exit(0)

    # Parse CLI arguments
    parser = argparse.ArgumentParser(description="Stage 5 Bulk Modification Framework")
    parser.add_argument("--input-file", type=str, default=str(input_file_candidate))
    parser.add_argument("--max-groups", type=int, default=None, help="Limit update batch to first N rule groups")
    parser.add_argument("--max-mls", type=int, default=None, help="Limit update batch to first N MLS IDs")
    parser.add_argument("--debug", action="store_true", help="Perform dry-run validation simulation")
    parser.add_argument("--do-update", action="store_true", help="Execute live database updates")

    args, unknown = parser.parse_known_args()

    target_csv_path = Path(args.input_file)
    if not target_csv_path.is_absolute():
        target_csv_path = current_dir / target_csv_path

    # Inspect and apply batch filtering
    active_csv_path = inspect_and_filter_payload(target_csv_path, max_groups=args.max_groups, max_mls=args.max_mls)

    # Interactive prompt before live updates
    if args.do_update:
        print("\n" + "=" * 70)
        print("⚠️ CONFIRMATION REQUIRED: You are about to execute LIVE database updates")
        print(f"   Target Payload File: {active_csv_path}")
        print("=" * 70)
        confirm = input("Type 'yes' or 'y' to proceed with Stage 5 bulk database update: ").strip().lower()
        if confirm not in ["yes", "y"]:
            logger.info("⛔ Live update aborted by user.")
            sys.exit(0)

    try:
        import bulk_map_tool
    except ImportError as e:
        logger.error(f"❌ Missing required dependency 'bulk_map_tool': {e}")
        logger.error(f"Configured TOOLS_REPO_PATH: '{tools_repo}'")
        sys.exit(1)

    passthrough_args = ["--input-file", str(active_csv_path)]
    if args.debug:
        passthrough_args.append("--debug")
    if args.do_update:
        passthrough_args.append("--do-update")

    logger.info(f"Handing off execution to bulk_map_tool with args: {' '.join(passthrough_args)}")
    sys.argv = ["bulk_map_tool.py"] + passthrough_args

    # Execute underlying bulk_map_tool main loop
    bulk_map_tool.main()


if __name__ == "__main__":
    try:
        run_stage_5()
    except (KeyboardInterrupt, SystemExit):
        force_terminal_exit()