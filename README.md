# 🚀 MLS Rule Migration & Standardization Execution Guide

This repository (`rules_cleanup`) contains the end-to-end automated pipeline required to standardize legacy custom MLS rule functions into generic targets (`PascalCase`, e.g., `ListStatusIdV1`) across Staging and Production infrastructure, finalize database audit ledgers, and maintain a synchronized `dm-consolidated-rules` Git repository.

---

## 📋 Prerequisites & System Requirements

Before executing the migration pipeline, ensure your system meets the following requirements:

### Requirements & Tools

- **Python 3.10+** (Python 3.12 recommended for binary wheel compatibility)
- **Git** (configured with access to Keller Williams internal repositories)
- **SSH Client & Keys** with access to Staging and Production bastion hosts (`bas-stage-cs.data.kw.com` / `sdh.data.kw.com`)
- **Network Access**: Internal network or VPN access required to reach PostgreSQL instances, microservices, and Jenkins hosts.

### Sibling Repository Dependencies

This project relies on local sibling repositories referenced in `.env.bulk` and `.env`. Ensure the following repositories are cloned adjacent to this project directory:

- `eim-slp-tools`
- `eim-utilities-pip`
- `eim-snowflake-id`
- `dm-consolidated-rules`

---

## 📦 Installation & Setup

### 1. Clone the Repository

```bash
git clone git@github.com:kwri/rules_cleanup.git
cd rules_cleanup
```

### 2. Set Up a Virtual Environment

```text
# Windows (PowerShell / CMD):
py -3.12 -m venv .venv

# macOS / Linux:
python3.12 -m venv .venv

# Activate the environment:
# Windows (PowerShell):
.\.venv\Scripts\Activate.ps1

# Windows (Git Bash):
source .venv/Scripts/activate

# macOS / Linux:
source .venv/bin/activate
```

### 3. Install Dependencies

```bash
python -m pip install --upgrade pip
pip install -r requirements.txt
```

## ⚙️ Environment Configuration

The pipeline requires two configuration files in the project root: `.env` and `.env.bulk`.

### `.env`

```dotenv
# Database Credentials (Staging & Production Verification)
DB_NAME=mls_admin
DB_USER=your_username            # Mandatory: Used for audit trail attribution in API calls and ledger tracking
DB_PASSWORD=your_password
DB_HOST=pg-sdk-stage-ext.data.kw.com
STAGE_DB_HOST=pg-sdk-stage-ext.data.kw.com
PROD_DB_HOST=pg-sdk-prod-ext.data.kw.com

# SSH / Bastion Tunnel Configuration
SSH_HOST=bas-stage-cs.data.kw.com
STAGE_SSH_HOST=bas-stage-cs.data.kw.com
PROD_SSH_HOST=bas-prod-cs.data.kw.com
SSH_USER=your_ssh_username
SSH_KEY_PATH=C:/Users/your.username/path/to/ssh_key.pem
SSH_KEY_PASSPHRASE=your_key_passphrase

# Remote Promotion Host (SDH Server)
REMOTE_SSH_HOST=sdh.data.kw.com
REMOTE_SSH_USER=your_ssh_username
REMOTE_SSH_KEY_PATH=C:/Users/your.username/path/to/ssh_key.pem
REMOTE_SSH_KEY_PASSPHRASE=your_key_passphrase
# Optional: defaults automatically to user-isolated path to prevent permission collisions:
# REMOTE_MANIFEST_PATH=/eim-mls-admin-ms/devtools/source_promotion/promotion_sources_your_user.txt

# Local Repository Paths
REPO_PATH=C:/Users/your.username/Documents/GitHub/dm-consolidated-rules

# Microservice Endpoints & Keys
MLS_ADMIN_STAGE_URL=https://stage-ext-ms.data.kw.com/v1/mls-admin
MLS_ADMIN_PROD_URL=https://prod-ext-ms.data.kw.com/v1/mls-admin
MLS_ADMIN_API_KEY=your_mls_admin_api_key

# Jenkins Integration (RETS Ingestion Jobs & Sync)
JENKINS_USER=your.email@kw.com
JENKINS_STAGE_URL=https://jenkins-stage.data.kw.com/
JENKINS_STAGE_TOKEN=your_jenkins_stage_token
JENKINS_PROD_URL=https://jenkins-prod.data.kw.com/
JENKINS_PROD_TOKEN=your_jenkins_prod_token

# Google BigQuery (Landing Verification)
GCP_PROJECT_ID=stream-listing-prod
GOOGLE_APPLICATION_CREDENTIALS=C:/Users/your.username/path/to/gcp_creds.json
```

### `.env.bulk`

```dotenv
MS_HOST=https://stage-ext-ms.data.kw.com/v1.0.3
MS_API_KEY=your_mls_admin_api_key
LOG_LEVEL=DEBUG

# Local Sibling Repository Paths
TOOLS_REPO_PATH=C:/Users/your.username/Documents/GitHub/eim-slp-tools
UTILS_REPO_PATH=C:/Users/your.username/Documents/GitHub/eim-utilities-pip
SNOWFLAKE_REPO_PATH=C:/Users/your.username/Documents/GitHub/eim-snowflake-id
```

## 🏃 Execution Methods

You can execute the migration pipeline either automatically using the master orchestrator script or manually stage by stage.

### Method 1: Master Orchestrator (Recommended)

`run_pipeline.py` executes the entire pipeline sequentially (Stages 1 through 11), handling live DB checks, automatic reverse promotion (Prod → Stage), mutation extractions, Stage DB mutations, Stage pre-ingestion RETS sync, Production promotion, BigQuery wait timers, interactive soft-deletions, and Git repository sync.

#### Interactive Mode (Default)

Runs with interactive (y/N) confirmation prompts before mutating Staging DB, disabling Production jobs, promoting, soft-deleting rules, and committing code:

```bash
python run_pipeline.py -l 9999
```

#### Starting From a Specific Stage (`--start-stage`)

If execution was interrupted or you want to resume from an intermediate point without re-clustering:

```bash
# Resume from Stage 3 (Batch manifest generation)
python run_pipeline.py --start-stage 3

# Resume from Stage 3a (Bulk Reverse Promotion)
python run_pipeline.py --start-stage 3a

# Resume from Stage 5 (Bulk DB Update)
python run_pipeline.py --start-stage 5
```

#### Automated / Headless CI Mode

Skips interactive prompts and automatically sleeps for BigQuery log landing:

```bash
python run_pipeline.py -l 9999 --auto-approve --wait-mins 5
```

#### Command Flags for `run_pipeline.py`

- `-s, --start-stage <str>`: Pipeline stage to start execution from (1, 2, 3, 3a, 4, 5, 5a, 6, 7, 8, 9, 10, 11). Default: 1.
- `-l, --limit <int>`: Batch limit parameter for Stage 1 AST clustering (Default: 9999).
- `--auto-approve`: Bypasses manual (y/N) confirmation prompts.
- `--wait-mins <int>`: Minutes to pause after triggering reprocess downloads (Stage 8) before verifying BigQuery landing (Stage 9) (Default: 5).

### Method 2: Manual / Sequential Stage Execution

If you need to execute specific stages individually in your terminal, run the scripts in the following exact numerical order:

```bash
# Stage 1: Group Identical Legacy Custom Rules by AST Code Structure
python 1_group_similar_rules.py -l 10

# Stage 2: Preview & Create Active Generic Target Rules
python 2_create_generic_target_rules.py               # Step 1: Generate preview CSV & inspect targets
python 2_create_generic_target_rules.py --do-creation  # Step 2: Register missing target rules via API

# Stage 3: Generate Consolidation Blueprint & Target MLS CSV Manifests
python 3_create_migration_batch.py

# Stage 3a: Reverse-Promote Live Production Process Maps to Staging
python 3_a_reverse_promote_maps.py

# Stage 4: Extract Final Targeted Process Maps from Staging DB
python 4_generate_targeted_csv.py

# Stage 5: Staging Bulk DB UI Map Updates (Dry-Run First, Then Live Update)
python 5_run_bulk_update.py --input-file temp-data/final_targeted_rules.csv --debug
python 5_run_bulk_update.py --input-file temp-data/final_targeted_rules.csv --do-update

# Stage 5a: Trigger Stage Ingestion Downloads & RETS Sync Jobs
python 5_a_trigger_downloads_stage.py

# Stage 6: Disable Production Ingestion Jobs & Sync Scheduler
python 6_disable_ingestion_jobs.py

# Stage 7: Production Promotion Engine (Drafts Slack Message, Stages Manifest, Remote SDH Runner)
python 7_run_bulk_promotion.py

# Stage 8: Enable Ingestion Jobs & Trigger 3-Day Reprocess Test Downloads
python 8_trigger_manual_downloads.py

# --- Wait ~5 Minutes to Allow Production BigQuery Data Landing ---

# Stage 9: Verify BigQuery Landing Logs & Re-Enable Verified API Schedules
python 9_verify_and_enable_injestion_jobs_api.py

# Stage 10: Safe Soft-Delete Unlinked Legacy Rules via API (Gated & Interactive)
python 10_soft_delete_legacy_rules.py

# Stage 11: Finalize DB Ledger, Sync Repo Files, Purge temp-data/, & Git Commit/Push
python 11_update_consolidated_repo.py
```

## 📋 Script Execution Sequence Reference

- **`1_group_similar_rules.py`**: Connects to DB, parses Python rule ASTs, clusters identical legacy custom rules, locks target rules as IN_PROGRESS in public.processed_rules_ledger, and outputs structural comparison groups.

- **`2_create_generic_target_rules.py`**: Resolves generic target names (e.g., ListStatusIdV1, AgentOfficeKeysV2), generates a preview CSV, and registers missing generic rules in Staging via Microservice API.

- **`3_create_migration_batch.py`**: Correlates candidate rules across Staging DB to generate consolidation_blueprint.csv, batch_mls_targets.csv, and safe_to_delete.csv.

- **`3_a_reverse_promote_maps.py`**: Connects to the SDH deployment jump host via SSH, initializes reverse promotion (set_promotion prod stage), and promotes live Production process maps for all batch MLS IDs into Staging, ensuring Staging matches Production before any modifications.

- **`4_generate_targeted_csv.py`**: Traverses Staging DB process maps, swaps legacy rules for generic targets, and writes final_targeted_rules.csv and promotion_sources.txt. Note: Only MLS feeds that have actual mutated records in final_targeted_rules.csv are written into promotion_sources.txt, preventing untouched feeds from being promoted.

- **`5_run_bulk_update.py`**: Applies bulk JSON schema updates directly to the Staging database via microservice API handoff (bulk_map_tool).

- **`5_a_trigger_downloads_stage.py`**: Triggers Stage ingestion verification downloads. For active RETS feeds, it runs:

  - `update_sync_time_to_yesterdays` (SOURCE_ID=sa_mls_id, STAGE_FLAG=stage)
  - `metadataSync-STAGE` (SOURCE_ID=sa_mls_id, SOURCE_NAME=sa_mls_id_str, LOAD_TYPE=full, STAGE_FLAG=stage)
  - Incremental ingestion runner builds.

  Gracefully skips inactive sources (mls_status_id != 2) without halting execution.

- **`6_disable_ingestion_jobs.py`**: Disables active Production Jenkins jobs (with CSRF crumbs) and API download schedules, and triggers Download Manager Scheduler Sync prior to promotion.

- **`7_run_bulk_promotion.py`**: Formats a Markdown Slack notification table and copies it to the clipboard, stages an isolated remote manifest (promotion_sources_<username>.txt) over SFTP, and executes the batch promotion loop on SDH using tr -d "\r" to eliminate line-ending issues.

- **`8_trigger_manual_downloads.py`**: Re-enables ingestion schedules and triggers parallel 3-day reprocessing ranges in Production, saving batch logs to triggered_downloads_log_api.csv.

- **`9_verify_and_enable_injestion_jobs_api.py`**: Queries BigQuery landing tables (stream-listing-prod.mls_download.*) by batch_id, validates row counts, and restores scheduled API download jobs for clean MLS targets. Automatically skipped for RETS-only batches.

- **`10_soft_delete_legacy_rules.py`**: Performs dual-environment DB dependency checks (Staging & Prod) to verify candidate rules have 0 active mapping references before soft-deleting them interactively via API. Production deletion is strictly gated and will only prompt if Staging soft-deletion succeeds.

- **`11_update_consolidated_repo.py`**: Marks evaluated rules as COMPLETED in public.processed_rules_ledger, extracts active and archived rules into ui-rules/active/ and ui-rules/archived/, clears read-only locks to purge temp-data/, and executes git add, commit, and push on the dm-consolidated-rules repository.

## 📂 Key State & Artifact Files in `temp-data/`

During pipeline execution, intermediate files are managed inside temp-data/ (and automatically purged upon completion of Stage 11):

- **`consolidation_blueprint.csv`**: Master blueprint matrix mapping legacy rule names (old_name) to target generic names (generic_rule_name).

- **`batch_mls_targets.csv`**: Target MLS feeds requiring map replacement and reverse promotion.

- **`final_targeted_rules.csv`**: Target CSV payload generated in Stage 4 containing exact JSON field paths and function replacements for the Stage 5 bulk update.

- **`promotion_sources.txt`**: Numerically sorted list of affected MLS IDs staged on the SDH remote server for Stage 7 promotion. Only includes sources with actual mutations in final_targeted_rules.csv.

- **`safe_to_delete.csv`**: Unreferenced rules safe for soft-deletion in Stage 10.

- **`triggered_downloads_stage_log.csv`**: Execution log from Stage 5a recording Stage download trigger statuses.

- **`triggered_downloads_log_api.csv`**: Operational log generated in Stage 8 storing batch_id values queried during Stage 9 BigQuery verification.

## 🛡️ Emergency Rollback & Safety Protocols

### Dry-Run Verification

Always execute Stage 5 with --debug prior to passing --do-update to inspect dry-run JSON payload mutations without writing to the database:

```bash
python 5_run_bulk_update.py --input-file temp-data/final_targeted_rules.csv --debug
```

### Emergency Staging UI Map Rollback

If a Staging DB update must be reverted prior to Stage 10 deletion, use the rollback utility to generate reverse_targeted_rules.csv and re-apply original legacy rule functions:

```bash
# 1. Generate reverse targeted CSV payload
python revert_mapping_to_old_rules.py

# 2. Re-apply legacy rule functions in Staging DB
python 5_run_bulk_update.py --input-file temp-data/reverse_targeted_rules.csv --do-update
```

### Baseline Reset via Reverse Promotion (Prod → Stage)

If Staging process map associations lose alignment or contain soft-deleted targets, restore the exact Production configurations back into Staging before running Stage 1:

1. SSH into the SDH remote deployment server (sdh.data.kw.com) and navigate to source promotion:

   ```bash
   s_p
   set_promotion prod stage
   ```

2. Re-promote exact active Production process maps back into Staging for target feeds:

   ```bash
   for id in 193 430 439 496; do python promote_source.py -s process "$id" -y; done
   ```

3. Reset locked ledger status back to PENDING in Stage DB (mls_admin):

   ```sql
   UPDATE public.processed_rules_ledger
   SET status = 'PENDING', updated_at = CURRENT_TIMESTAMP
   WHERE status = 'IN_PROGRESS' AND DATE(created_at) = CURRENT_DATE;
   ```

### Recovery from Paused Pipeline Ingestion Jobs

If pipeline execution halts after Stage 6 (where download jobs are disabled), re-enable verified ingestion schedules by running Stage 9 directly:

```bash
python 9_verify_and_enable_injestion_jobs_api.py
```
