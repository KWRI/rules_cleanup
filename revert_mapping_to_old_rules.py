"""
Text-Based Reverse Mapping Converter (Emergency Rollback Utility).

Reads 'temp-data/consolidation_blueprint.csv' (or 'migration_blueprint.csv') to dynamically map
newly standardized generic rule names back to their exact legacy string formats.
Performs structured CSV field substitutions across 'raw_targeted_rules.csv'
and outputs the rollback payload to 'reverse_targeted_rules.csv'.

Usage:
  1. Generate reverse CSV payload:
     python revert_mapping_to_old_rules.py

  2. Apply rollback to Staging DB:
     python 5_run_bulk_update.py --input-file temp-data/reverse_targeted_rules.csv --do-update

Logs execution details to 'logs/pipeline_YYYY-MM-DD.log'.
"""

import os
import sys
import csv
import json
import signal
import logging
import warnings
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

from pipeline_logger import setup_logger

# --- OS-LEVEL INSTANT TERMINAL EXIT ---
def force_terminal_exit(sig=None, frame=None):
    print("\n⛔ [TERMINAL ABORT] Killing process tree immediately...")
    os._exit(1)

signal.signal(signal.SIGINT, force_terminal_exit)
if hasattr(signal, "SIGBREAK"):
    signal.signal(signal.SIGBREAK, force_terminal_exit)

warnings.filterwarnings("ignore")

current_dir = Path(__file__).resolve().parent
load_dotenv(dotenv_path=current_dir / ".env")

logger = setup_logger("RevertMappings")

temp_data_dir = current_dir / "temp-data"
raw_rules_path = temp_data_dir / "raw_targeted_rules.csv"
output_reverse_path = temp_data_dir / "reverse_targeted_rules.csv"


def resolve_blueprint_path() -> Path | None:
    """Finds active consolidation or migration blueprint file."""
    candidates = [
        temp_data_dir / "consolidation_blueprint.csv",
        temp_data_dir / "migration_blueprint.csv",
    ]
    for path in candidates:
        if path.exists():
            return path
    return None


def generate_reverse_targeted_rules() -> None:
    logger.info("============================================================")
    logger.info("🔄 REVERSE TARGETED RULES GENERATOR INITIALIZED (ROLLBACK)")
    logger.info("============================================================")

    blueprint_path = resolve_blueprint_path()

    # --- Validation Checks ---
    if not blueprint_path or not blueprint_path.exists():
        logger.error("❌ Migration blueprint missing in 'temp-data/'.")
        logger.error("   Expected 'consolidation_blueprint.csv' or 'migration_blueprint.csv'.")
        sys.exit(1)

    if not raw_rules_path.exists():
        logger.error(f"❌ Raw targeted rules file missing at: {raw_rules_path}")
        logger.error("   Ensure Stage 4 generated 'raw_targeted_rules.csv' first.")
        sys.exit(1)

    logger.info(f"[1/2] Loading reversal mappings from blueprint: '{blueprint_path.name}'...")

    # Load reverse mappings: clean_new_name -> old_name
    reverse_mappings = {}
    with open(blueprint_path, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            old_name = row.get("old_name", "").strip()
            new_name = (
                row.get("target_new_name") or
                row.get("generic_rule_name") or
                row.get("new_name") or ""
            ).strip()

            if not old_name or not new_name:
                continue

            clean_new_name = new_name[:-3] if new_name.endswith(".py") else new_name

            # Only track mapping if rule name was modified
            if old_name != clean_new_name:
                reverse_mappings[clean_new_name] = old_name

    if not reverse_mappings:
        logger.warning("⚠️ No modified rule mappings found in blueprint to revert.")
        return

    logger.info(f"   * Loaded {len(reverse_mappings)} reverse rule mapping definition(s).")
    logger.info("[2/2] Reading raw rules and executing structured CSV rule function reversals...")

    total_replacements = 0
    reverted_rows = []

    with open(raw_rules_path, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames

        for row in reader:
            rule_func_raw = row.get("rule_func", "")

            if rule_func_raw:
                try:
                    funcs = json.loads(rule_func_raw)
                    if isinstance(funcs, list):
                        updated_funcs = []
                        for func in funcs:
                            if func in reverse_mappings:
                                old_func = reverse_mappings[func]
                                updated_funcs.append(old_func)
                                total_replacements += 1
                                logger.info(f"   * Reverting '{func}' -> '{old_func}' in MLS {row.get('mls_id')}")
                            else:
                                updated_funcs.append(func)

                        row["rule_func"] = json.dumps(updated_funcs)
                    elif isinstance(funcs, str) and funcs in reverse_mappings:
                        old_func = reverse_mappings[funcs]
                        row["rule_func"] = json.dumps([old_func])
                        total_replacements += 1

                except (json.JSONDecodeError, TypeError):
                    for new_name, old_name in reverse_mappings.items():
                        if new_name in rule_func_raw:
                            rule_func_raw = rule_func_raw.replace(new_name, old_name)
                            total_replacements += 1
                    row["rule_func"] = rule_func_raw

            reverted_rows.append(row)

    # Output to reverse target file
    with open(output_reverse_path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(reverted_rows)

    logger.info("============================================================")
    logger.info("🎉 SUCCESS: REVERSE MAPPING FILE GENERATED")
    logger.info("============================================================")
    logger.info(f"   Source File Managed : {raw_rules_path.name} (Untouched)")
    logger.info(f"   Target File Created : {output_reverse_path.name}")
    logger.info(f"   Total Function Reversals : {total_replacements}")
    logger.info("============================================================")


if __name__ == "__main__":
    try:
        generate_reverse_targeted_rules()
    except (KeyboardInterrupt, SystemExit):
        force_terminal_exit()