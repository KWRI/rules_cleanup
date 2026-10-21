"""
MLS-Aware Text-Based Reverse Mapping Converter (Emergency Rollback Utility).

Reads 'temp-data/consolidation_blueprint.csv' to dynamically map newly standardized
generic rule names back to their exact, source-specific legacy string formats per MLS ID.
Performs structured CSV field substitutions across 'raw_targeted_rules.csv'
and outputs the precise rollback payload to 'reverse_targeted_rules.csv'.

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
from collections import defaultdict
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
    logger.info("🔄 MLS-AWARE REVERSE TARGETED RULES GENERATOR INITIALIZED")
    logger.info("============================================================")

    blueprint_path = resolve_blueprint_path()

    if not blueprint_path or not blueprint_path.exists():
        logger.error("❌ Migration blueprint missing in 'temp-data/'.")
        logger.error("   Expected 'consolidation_blueprint.csv' or 'migration_blueprint.csv'.")
        sys.exit(1)

    if not raw_rules_path.exists():
        logger.error(f"❌ Raw targeted rules file missing at: {raw_rules_path}")
        logger.error("   Ensure Stage 4 generated 'raw_targeted_rules.csv' first.")
        sys.exit(1)

    logger.info(f"[1/2] Loading reversal candidate mappings from blueprint: '{blueprint_path.name}'...")

    # Group candidates: generic_rule_name -> list of old_names
    generic_to_olds = defaultdict(list)
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

            if old_name != clean_new_name and old_name not in generic_to_olds[clean_new_name]:
                generic_to_olds[clean_new_name].append(old_name)

    logger.info(f"   * Loaded {len(generic_to_olds)} generic target rule definitions.")

    def resolve_exact_old_name(mls_id, gen_func_name):
        clean_gen = gen_func_name[:-3] if gen_func_name.endswith(".py") else gen_func_name
        candidates = generic_to_olds.get(clean_gen, [])

        if not candidates:
            return gen_func_name

        mls_str = str(int(mls_id)).strip() if (mls_id and str(mls_id).isdigit()) else ""

        if mls_str:
            # 1. Match candidates starting with MLS ID (e.g. '193AgentOfficeKeys' for MLS 193)
            for c in candidates:
                if c.startswith(mls_str):
                    return c
            # 2. Match candidates containing MLS ID
            for c in candidates:
                if mls_str in c:
                    return c

        # 3. Fallback to first candidate
        return candidates[0]

    logger.info("[2/2] Reading raw rules and executing MLS-Aware rule function reversals...")

    total_replacements = 0
    reverted_rows = []

    with open(raw_rules_path, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames

        for row in reader:
            mls_id = row.get("mls_id")
            rule_func_raw = row.get("rule_func", "")

            if rule_func_raw:
                try:
                    funcs = json.loads(rule_func_raw)
                    if isinstance(funcs, list):
                        updated_funcs = []
                        for func in funcs:
                            old_func = resolve_exact_old_name(mls_id, func)
                            if old_func != func:
                                total_replacements += 1
                                logger.info(f"   * MLS {mls_id:3s} | Reverted '{func}' -> '{old_func}'")
                            updated_funcs.append(old_func)

                        row["rule_func"] = json.dumps(updated_funcs)

                except (json.JSONDecodeError, TypeError):
                    for gen_name in generic_to_olds.keys():
                        if gen_name in rule_func_raw:
                            old_func = resolve_exact_old_name(mls_id, gen_name)
                            rule_func_raw = rule_func_raw.replace(gen_name, old_func)
                            total_replacements += 1
                            logger.info(f"   * MLS {mls_id:3s} | Reverted '{gen_name}' -> '{old_func}'")
                    row["rule_func"] = rule_func_raw

            reverted_rows.append(row)

    # Output to reverse target file
    with open(output_reverse_path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(reverted_rows)

    logger.info("============================================================")
    logger.info("🎉 SUCCESS: MLS-AWARE REVERSE MAPPING FILE GENERATED")
    logger.info("============================================================")
    logger.info(f"   Source File Managed      : {raw_rules_path.name}")
    logger.info(f"   Target File Created      : {output_reverse_path.name}")
    logger.info(f"   Total Function Reversals : {total_replacements}")
    logger.info("============================================================")


if __name__ == "__main__":
    try:
        generate_reverse_targeted_rules()
    except (KeyboardInterrupt, SystemExit):
        force_terminal_exit()
