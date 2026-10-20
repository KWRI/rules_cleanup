"""
Master Migration Pipeline Orchestrator.

Sequentially executes Stages 1 through 11 with built-in error handling,
preview safety review gates, dry-run verification steps, timing pause prompts,
and user confirmation steps.

Usage:
    python run_pipeline.py                               # Interactive mode (default AST limit: 9999)
    python run_pipeline.py --limit 10                    # Custom AST limit
    python run_pipeline.py --auto-approve                # Headless CI/CD mode
    python run_pipeline.py --auto-approve --wait-mins 5 # Headless mode with 5-minute BigQuery wait gate
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

logger = setup_logger("MasterPipeline")


def run_stage(script_name: str, args: list[str] = None) -> bool:
    """Executes a pipeline script as a subprocess."""
    cmd = [sys.executable, script_name] + (args or [])
    logger.info("============================================================")
    logger.info(f"🚀 EXECUTING: {' '.join(cmd)}")
    logger.info("============================================================")

    result = subprocess.run(cmd)

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

    logger.info("============================================================")
    logger.info("🏁 STARTING AUTOMATED MLS MIGRATION PIPELINE (STAGES 1-11)")
    logger.info("============================================================")

    # ------------------------------------------------------------
    # STAGE 1: Group Similar Custom Rules by AST Equivalence
    # ------------------------------------------------------------
    if not run_stage("1_group_similar_rules.py", ["-l", str(args.limit)]):
        sys.exit(1)

    # ------------------------------------------------------------
    # STAGE 2: Standardize Generic Target Names & Register Missing Rules via API
    # ------------------------------------------------------------
    # Pass A: Preview Mode (Generates resolved_generic_targets.csv without DB mutations)
    if not run_stage("2_create_generic_target_rules.py"):
        sys.exit(1)

    if not args.auto_approve:
        if not prompt_user_confirmation("Stage 2 Preview complete. Inspect 'temp-data/resolved_generic_targets.csv'. Proceed with Rule Creation (--do-creation)?"):
            logger.warning("Pipeline paused by user prior to Stage 2 rule creation.")
            sys.exit(0)

    # Pass B: Execution Mode (Creates new generic rules via Microservice API)
    if not run_stage("2_create_generic_target_rules.py", ["--do-creation"]):
        sys.exit(1)

    # ------------------------------------------------------------
    # STAGE 3: Build Consolidation Blueprint & Target MLS CSVs
    # ------------------------------------------------------------
    if not run_stage("3_create_migration_batch.py"):
        sys.exit(1)

    # ------------------------------------------------------------
    # STAGE 4: Extract Raw Targeted Process Maps
    # ------------------------------------------------------------
    if not run_stage("4_generate_targeted_csv.py"):
        sys.exit(1)

    # Check if raw_targeted_rules.csv actually has pending mutation rows
    targeted_csv = Path("temp-data/raw_targeted_rules.csv")
    has_mutations = False
    if targeted_csv.exists():
        lines = [line.strip() for line in targeted_csv.read_text(encoding="utf-8").splitlines() if line.strip()]
        if len(lines) > 1:  # More than header row
            has_mutations = True

    if not has_mutations:
        logger.info("ℹ️ All target MLS rules are already standardized (no-op batch). Finalizing repository directly.")
        run_stage("11_sync_and_commit_repo.py")
        logger.info("🎉 No DB mutations needed. Ledger finalized and Git repo updated.")
        sys.exit(0)

    # ------------------------------------------------------------
    # STAGE 5: Staging DB Bulk Process Map Mutations
    # ------------------------------------------------------------
    logger.info("Running Stage 5 in DRY-RUN (--debug) mode for safety verification...")
    if not run_stage("5_run_bulk_update.py", ["--input-file", "temp-data/raw_targeted_rules.csv", "--debug"]):
        sys.exit(1)

    if not args.auto_approve:
        if not prompt_user_confirmation("Review dry-run output above. Proceed with LIVE Staging DB map update?"):
            logger.warning("Pipeline aborted by user prior to Staging DB mutation.")
            sys.exit(0)

    if not run_stage("5_run_bulk_update.py", ["--input-file", "temp-data/raw_targeted_rules.csv", "--do-update"]):
        sys.exit(1)

    # ------------------------------------------------------------
    # STAGE 6: Disable Active Ingestion Jobs & Sync Scheduler
    # ------------------------------------------------------------
    if not args.auto_approve:
        if not prompt_user_confirmation("Stage 5 complete. Proceed with DISABLING active Ingestion Jobs (Stage 6)?"):
            logger.warning("Pipeline paused by user prior to disabling ingestion jobs.")
            sys.exit(0)

    if not run_stage("6_disable_ingestion_jobs.py"):
        sys.exit(1)

    # ------------------------------------------------------------
    # STAGE 7: Bulk Production Promotion Engine
    # ------------------------------------------------------------
    if not args.auto_approve:
        if not prompt_user_confirmation("Ingestion jobs disabled. Proceed with PRODUCTION PROMOTION (Stage 7)?"):
            logger.warning("Pipeline paused by user prior to Production promotion.")
            sys.exit(0)

    if not run_stage("7_bulk_promotion_engine.py"):
        sys.exit(1)

    # ------------------------------------------------------------
    # STAGE 8: Re-Enable Jobs & Trigger Reprocess Downloads
    # ------------------------------------------------------------
    if not run_stage("8_trigger_downloads.py"):
        sys.exit(1)

    # Wait Gate for BigQuery Landing
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
            logger.info("💡 You can manually run 'python 9_verify_and_enable_api.py' when ready.")
            sys.exit(0)

    # ------------------------------------------------------------
    # STAGE 9: BigQuery Verification & API Scheduler Restoration
    # ------------------------------------------------------------
    if not run_stage("9_verify_and_enable_api.py"):
        logger.error("⛔ Stage 9 detected errors in BigQuery logs or failed execution. Pipeline stopped.")
        sys.exit(1)

    # ------------------------------------------------------------
    # STAGE 10: Safe Soft Delete via Microservice API (Interactive & Gated)
    # ------------------------------------------------------------
    if not run_stage("10_soft_delete_legacy_rules.py"):
        sys.exit(1)

    # ------------------------------------------------------------
    # STAGE 11: Repo DB Sync, DB Ledger Finalization & Git Commit/Push
    # ------------------------------------------------------------
    if not run_stage("11_sync_and_commit_repo.py"):
        sys.exit(1)

    logger.info("============================================================")
    logger.info("🎉 MASTER PIPELINE MIGRATION BATCH COMPLETED SUCCESSFULLY!")
    logger.info("============================================================")


if __name__ == "__main__":
    try:
        main()
    except (KeyboardInterrupt, SystemExit):
        force_terminal_exit()