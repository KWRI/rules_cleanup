"""
Master Migration Pipeline Orchestrator.

Sequentially executes Stages 1 through 11 with built-in error handling,
preview safety review gates, dry-run verification steps, timing pause prompts,
and user confirmation steps.

Usage:
    python run_pipeline.py                               # Interactive mode from Stage 1
    python run_pipeline.py --start-stage 3               # Start execution from Stage 3 onward
    python run_pipeline.py --start-stage 3a              # Start execution from Stage 3a onward
    python run_pipeline.py --auto-approve                # Headless CI/CD mode
"""

import sys
import time
import signal
import argparse
import subprocess
from pathlib import Path
from pipeline_logger import setup_logger

# --- OS-LEVEL INSTANT TERMINAL EXIT ---
def force_terminal_exit(sig=None, frame=None):
    print("\n⛔ [TERMINAL ABORT] Killing pipeline orchestrator immediately...")
    sys.exit(1)


signal.signal(signal.SIGINT, force_terminal_exit)
if hasattr(signal, "SIGBREAK"):
    signal.signal(signal.SIGBREAK, force_terminal_exit)

current_dir = Path(__file__).resolve().parent
logger = setup_logger("MasterPipeline")

# Defined pipeline execution order
STAGE_ORDER = [
    "1", "2", "3", "3a", "4", "5", "5a", "6", "7", "8", "9", "10", "11"
]


def should_run(stage_key: str, start_stage: str) -> bool:
    """Returns True if stage_key appears at or after start_stage in the pipeline sequence."""
    stage_key_clean = stage_key.lower().strip()
    start_stage_clean = start_stage.lower().strip()

    if start_stage_clean not in STAGE_ORDER:
        return True

    return STAGE_ORDER.index(stage_key_clean) >= STAGE_ORDER.index(start_stage_clean)


def run_stage(script_name: str, args: list[str] = None) -> bool:
    """Executes a pipeline script as a subprocess."""
    script_path = current_dir / script_name
    cmd = [sys.executable, str(script_path)] + (args or [])
    logger.info("============================================================")
    logger.info(f"🚀 EXECUTING: {' '.join(cmd)}")
    logger.info("============================================================")

    result = subprocess.run(cmd, cwd=str(current_dir))

    if result.returncode != 0:
        logger.error(f"❌ STAGE FAILED: '{script_name}' exited with status code {result.returncode}.")
        return False

    logger.info(f"✅ STAGE PASSED: '{script_name}' completed successfully.\n")
    return True


def prompt_user_confirmation(prompt_text: str) -> bool:
    """Prompts the operator for explicit Y/N confirmation."""
    try:
        answer = input(f"\n⚠️  {prompt_text} (y/N): ").strip().lower()
        return answer == "y"
    except (KeyboardInterrupt, EOFError):
        force_terminal_exit()


def main() -> None:
    if sys.platform == "win32" and hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except Exception:
            pass

    parser = argparse.ArgumentParser(description="Master MLS Rule Migration Pipeline Orchestrator")
    parser.add_argument(
        "-s", "--start-stage",
        type=str,
        default="1",
        dest="start_stage",
        help="Stage to start execution from (e.g. 1, 2, 3, 3a, 4, 5, 5a, 6, 7, 8, 9, 10, 11)"
    )
    parser.add_argument(
        "-l", "--limit",
        type=int,
        default=9999,
        dest="limit",
        help="Limit parameter for Stage 1 AST clustering (default: 9999)"
    )
    parser.add_argument(
        "--auto-approve",
        action="store_true",
        help="Skip interactive prompts"
    )
    parser.add_argument(
        "--wait-mins",
        type=int,
        default=5,
        help="Minutes to wait for BigQuery landing after Stage 8 (default: 5)"
    )
    args = parser.parse_args()

    start_stage = args.start_stage.lower().strip()
    if start_stage not in STAGE_ORDER:
        logger.error(f"❌ Invalid start stage '{args.start_stage}'. Valid options: {', '.join(STAGE_ORDER)}")
        sys.exit(1)

    logger.info("============================================================")
    logger.info(f"🏁 STARTING AUTOMATED MLS MIGRATION PIPELINE (FROM STAGE {start_stage.upper()})")
    logger.info("============================================================")

    # ------------------------------------------------------------
    # STAGE 1: Group Similar Custom Rules by AST Equivalence
    # ------------------------------------------------------------
    if should_run("1", start_stage):
        if not run_stage("1_group_similar_rules.py", ["-l", str(args.limit)]):
            sys.exit(1)

    # ------------------------------------------------------------
    # STAGE 2: Standardize Generic Target Names & Register via API
    # ------------------------------------------------------------
    if should_run("2", start_stage):
        if not run_stage("2_create_generic_target_rules.py"):
            sys.exit(1)

        if not args.auto_approve:
            if not prompt_user_confirmation("Stage 2 Preview complete. Inspect 'temp-data/resolved_generic_targets.csv'. Proceed with Rule Creation (--do-creation)?"):
                logger.warning("Pipeline paused by user prior to Stage 2 rule creation.")
                sys.exit(0)

        if not run_stage("2_create_generic_target_rules.py", ["--do-creation"]):
            sys.exit(1)

    # ------------------------------------------------------------
    # STAGE 3: Build Consolidation Blueprint & Target MLS CSVs
    # ------------------------------------------------------------
    if should_run("3", start_stage):
        if not run_stage("3_create_migration_batch.py"):
            sys.exit(1)

    # ------------------------------------------------------------
    # STAGE 3a: Reverse Promote Production Process Maps to Staging
    # ------------------------------------------------------------
    if should_run("3a", start_stage):
        if not run_stage("3_a_reverse_promote_maps.py", ["-y"] if args.auto_approve else []):
            sys.exit(1)

    # ------------------------------------------------------------
    # STAGE 4: Extract Final Targeted Process Maps
    # ------------------------------------------------------------
    if should_run("4", start_stage):
        if not run_stage("4_generate_targeted_csv.py"):
            sys.exit(1)

    # Validate mutation payload presence
    targeted_csv = current_dir / "temp-data" / "final_targeted_rules.csv"
    if not targeted_csv.exists():
        targeted_csv = current_dir / "temp-data" / "raw_targeted_rules.csv"

    has_mutations = False
    if targeted_csv.exists():
        lines = [line.strip() for line in targeted_csv.read_text(encoding="utf-8").splitlines() if line.strip()]
        if len(lines) > 1:
            has_mutations = True

    if not has_mutations and should_run("5", start_stage):
        logger.info("ℹ️ All target MLS rules are already standardized (no-op batch). Finalizing repository directly.")
        run_stage("11_update_consolidated_repo.py")
        logger.info("🎉 No DB mutations needed. Ledger finalized and Git repo updated.")
        sys.exit(0)

    # ------------------------------------------------------------
    # STAGE 5: Staging DB Bulk Process Map Mutations
    # ------------------------------------------------------------
    if should_run("5", start_stage):
        logger.info("Running Stage 5 in DRY-RUN (--debug) mode for safety verification...")
        if not run_stage("5_run_bulk_update.py", ["--input-file", str(targeted_csv), "--debug"]):
            sys.exit(1)

        if not args.auto_approve:
            if not prompt_user_confirmation("Review dry-run output above. Proceed with LIVE Staging DB map update?"):
                logger.warning("Pipeline aborted by user prior to Staging DB mutation.")
                sys.exit(0)

        if not run_stage("5_run_bulk_update.py", ["--input-file", str(targeted_csv), "--do-update"]):
            sys.exit(1)

    # ------------------------------------------------------------
    # STAGE 5a: Trigger Stage Ingestion Downloads & RETS Sync Jobs
    # ------------------------------------------------------------
    if should_run("5a", start_stage):
        if not run_stage("5_a_trigger_downloads_stage.py"):
            logger.warning("⚠️ Stage 5a reported non-fatal issues. Proceeding with pipeline...")

    # ------------------------------------------------------------
    # STAGE 6: Disable Active Ingestion Jobs & Sync Scheduler
    # ------------------------------------------------------------
    if should_run("6", start_stage):
        if not args.auto_approve:
            if not prompt_user_confirmation("Stage 5 & 5a complete. Proceed with DISABLING active Ingestion Jobs (Stage 6)?"):
                logger.warning("Pipeline paused by user prior to disabling ingestion jobs.")
                sys.exit(0)

        if not run_stage("6_disable_ingestion_jobs.py"):
            sys.exit(1)

    # ------------------------------------------------------------
    # STAGE 7: Bulk Production Promotion Engine
    # ------------------------------------------------------------
    if should_run("7", start_stage):
        if not run_stage("7_run_bulk_promotion.py"):
            sys.exit(1)

    # ------------------------------------------------------------
    # STAGE 8: Re-Enable Jobs & Trigger Reprocess Downloads
    # ------------------------------------------------------------
    if should_run("8", start_stage):
        if not run_stage("8_trigger_manual_downloads.py"):
            sys.exit(1)

        # Wait gate for BigQuery landing
        logger.info("============================================================")
        logger.info("⏳ REPROCESS DOWNLOADS TRIGGERED IN STAGE 8")
        logger.info(f"   Downloads typically take up to {args.wait_mins} minutes to land in BigQuery.")
        logger.info("============================================================")

        if args.auto_approve:
            wait_seconds = args.wait_mins * 60
            logger.info(f"🤖 Auto-approve mode active. Sleeping for {args.wait_mins} minute(s)...")
            for remaining in range(wait_seconds, 0, -60):
                logger.info(f"⏳ Time remaining: {remaining // 60} minute(s)...")
                time.sleep(60)
            logger.info("✅ Wait complete. Proceeding to Stage 9 BigQuery verification.")
        else:
            if not prompt_user_confirmation(f"Have you allowed ~{args.wait_mins} minutes for downloads to land in BigQuery? Ready to verify (Stage 9)?"):
                logger.warning("Pipeline paused by user prior to BigQuery verification.")
                logger.info("💡 You can manually run 'python 9_verify_and_enable_injestion_jobs_api.py' when ready.")
                sys.exit(0)

    # ------------------------------------------------------------
    # STAGE 9: BigQuery Verification & API Scheduler Restoration
    # ------------------------------------------------------------
    if should_run("9", start_stage):
        if not run_stage("9_verify_and_enable_injestion_jobs_api.py"):
            logger.error("⛔ Stage 9 detected errors or failed execution. Pipeline stopped.")
            sys.exit(1)

    # ------------------------------------------------------------
    # STAGE 10: Safe Soft Delete via Microservice API (Interactive & Gated)
    # ------------------------------------------------------------
    if should_run("10", start_stage):
        if not run_stage("10_soft_delete_legacy_rules.py"):
            sys.exit(1)

    # ------------------------------------------------------------
    # STAGE 11: Repo DB Sync, DB Ledger Finalization & Git Commit/Push
    # ------------------------------------------------------------
    if should_run("11", start_stage):
        if not run_stage("11_update_consolidated_repo.py"):
            sys.exit(1)

    logger.info("============================================================")
    logger.info("🎉 MASTER PIPELINE MIGRATION BATCH COMPLETED SUCCESSFULLY!")
    logger.info("============================================================")


if __name__ == "__main__":
    try:
        main()
    except (KeyboardInterrupt, SystemExit):
        force_terminal_exit()