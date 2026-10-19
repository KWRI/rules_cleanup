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

1. **Clone the repository:**
   ```bash
   git clone git@github.com:kwri/rules_cleanup.git
   cd rules_cleanup

   ```

2. **Set up a Virtual Environment:**
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
3. **Install Dependencies:**
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
SSH_USER=your_ssh_username
SSH_KEY_PATH=C:/Users/your.username/path/to/ssh_key.pem
SSH_KEY_PASSPHRASE=your_key_passphrase

# Remote Promotion Host (SDH Server)
REMOTE_SSH_HOST=sdh.data.kw.com
REMOTE_SSH_USER=your_ssh_username
REMOTE_SSH_KEY_PATH=C:/Users/your.username/path/to/ssh_key.pem
REMOTE_SSH_KEY_PASSPHRASE=your_key_passphrase
REMOTE_MANIFEST_PATH=/eim-mls-admin-ms/devtools/source_promotion/promotion_sources.txt

# Local Repository Paths
REPO_PATH=C:/Users/your.username/Documents/GitHub/dm-consolidated-rules

# Microservice Endpoints & Keys
MLS_ADMIN_STAGE_URL=https://stage-ext-ms.data.kw.com/v1/mls-admin
MLS_ADMIN_PROD_URL=https://prod-ext-ms.data.kw.com/v1/mls-admin
MLS_ADMIN_API_KEY=your_mls_admin_api_key

# Jenkins Integration (RETS Ingestion Jobs)
JENKINS_USER=your.email@kw.com
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

You can execute the migration pipeline either automatically using the master orchestrator script or manually stage-by-stage.

### Method 1: Master Orchestrator (Recommended)

`run_pipeline.py` executes the entire pipeline sequentially (Stages 1–11), handling live DB checks, dry-run safety gates, no-op MLS fast-forwarding, BigQuery wait timers, interactive soft-deletions, and Git repository sync automatically.

#### Interactive Mode (Default)

Runs with interactive `(y/N)` confirmation prompts before mutating Staging DB, disabling Production jobs, promoting, soft-deleting rules, and committing code:

```bash
python run_pipeline.py -l 9999

```

#### Automated / CI Mode

Skips interactive prompts and automatically sleeps for BigQuery log landing:

```bash
python run_pipeline.py -l 9999 --auto-approve --wait-mins 5

```

#### Command Flags for `run_pipeline.py`

- `-l, --limit <int>`: Batch limit parameter for Stage 1 AST clustering (Default: `9999`).
- `--auto-approve`: Bypasses manual `(y/N)` confirmation prompts.
- `--wait-mins <int>`: Minutes to pause after triggering reprocess downloads (Stage 8) before verifying BigQuery landing (Stage 9) (Default: `5`).

### Method 2: Manual / Sequential Stage Execution

If you need to execute specific stages individually, run the scripts in the following exact numerical order:

```bash
# Stage 1: Group Identical Legacy Custom Rules by AST Code Structure
python 1_group_similar_rules.py -l 9999

# Stage 2: Derive Generic Target Names & Register Missing Generic Rules via API
python 2_create_generic_target_rules.py

# Stage 3: Generate Consolidation Blueprint & Target MLS CSV Manifests
python 3_create_migration_batch.py

# Stage 4: Extract Raw Targeted Process Maps from Staging DB
python 4_generate_targeted_csv.py

# Stage 5: Staging Bulk DB UI Map Updates (Dry-Run First, Then Live Update)
python 5_run_bulk_update.py --input-file temp-data/raw_targeted_rules.csv --debug
python 5_run_bulk_update.py --input-file temp-data/raw_targeted_rules.csv --do-update

# Stage 6: Disable Production Ingestion Jobs (Jenkins & API Service)
python 6_disable_ingestion_jobs.py

# Stage 7: Production Promotion Engine (Remote SDH SSH Shell Runner)
python 7_bulk_promotion_engine.py

# Stage 8: Enable Ingestion Jobs & Trigger 3-Day Reprocess Test Downloads
python 8_trigger_downloads.py

# --- Wait ~5 Minutes to Allow Production BigQuery Data Landing ---

# Stage 9: Verify BigQuery Landing Logs & Re-Enable Verified API Schedules
python 9_verify_and_enable_api.py

# Stage 10: Safe Soft-Delete Unlinked Legacy Rules via API (Gated & Interactive)
python 10_soft_delete_legacy_rules.py

# Stage 11: Finalize DB Ledger, Sync Repo Files, Purge temp-data/, & Git Push
python 11_sync_and_commit_repo.py

```

## 📋 Script Execution Sequence Reference

- **`1_group_similar_rules.py`**: Connects to DB, parses Python rule ASTs, clusters identical legacy custom rules, and outputs structural comparison groups.
- **`2_create_generic_target_rules.py`**: Resolves generic target names (e.g., `ListStatusIdV1`), registers any missing generic rules in Staging via Microservice API, and updates `public.processed_rules_ledger` to `IN_PROGRESS`.
- **`3_create_migration_batch.py`**: Correlates target rule names with active process maps to generate `consolidation_blueprint.csv` and `batch_mls_targets.csv`.
- **`4_generate_targeted_csv.py`**: Traverses process maps in Staging DB and generates mutation payload `raw_targeted_rules.csv` and `promotion_sources.txt`.
- **`5_run_bulk_update.py`**: Applies bulk JSON schema updates directly to the Staging database via microservice API handoff (`bulk_map_tool`).
- **`6_disable_ingestion_jobs.py`**: Pauses active Production Jenkins jobs (RETS) and API ingestion schedules prior to promotion.
- **`7_bulk_promotion_engine.py`**: Uploads deployment manifests via SFTP to the remote SDH host and runs interactive promotion scripts (`promote_source.py`).
- **`8_trigger_downloads.py`**: Re-enables ingestion schedules and triggers parallel 3-day reprocessing ranges in Production, saving batch logs to `triggered_downloads_log_api.csv`.
- **`9_verify_and_enable_api.py`**: Queries BigQuery landing tables (`mls_download`) by `batch_id`, validates row counts, and restores scheduled API download jobs for clean MLS targets.
- **`10_soft_delete_legacy_rules.py`**: Performs dual-environment DB dependency checks (Staging & Prod) to verify rules have 0 active mapping references before soft-deleting them interactively via API.
- **`11_sync_and_commit_repo.py`**: Marks evaluated rules as `COMPLETED` in `public.processed_rules_ledger`, extracts fresh rules to `ui-rules/active/` and `ui-rules/archived/`, purges `temp-data/`, and executes `git add`, `commit`, and `push` on the `dm-consolidated-rules` repository.

## 📂 Key State & Artifact Files in `temp-data/`

During pipeline execution, intermediate files are managed inside `temp-data/` (and automatically purged upon completion of Stage 11):

- **`consolidation_blueprint.csv`**: Master blueprint matrix mapping legacy rule names (`old_name`) to target generic names (`generic_rule_name`).
- **`raw_targeted_rules.csv`**: Target CSV payload generated in Stage 4 containing exact JSON field paths and function replacements for Stage 5 bulk update.
- **`promotion_sources.txt`**: Numerically sorted list of affected MLS IDs staged on the SDH remote server for Stage 7 promotion.
- **`triggered_downloads_log_api.csv`**: Operational log generated in Stage 8 storing `batch_id` values queried during Stage 9 BigQuery verification.

## 🛡️ Emergency Rollback & Safety Protocols

### Dry-Run Verification

Always execute **Stage 5** with `--debug` prior to passing `--do-update` to inspect dry-run JSON payload mutations without writing to the database:

```bash
python 5_run_bulk_update.py --input-file temp-data/raw_targeted_rules.csv --debug

```

### Emergency Staging UI Map Rollback

If a Staging DB update must be reverted prior to Stage 10 deletion, use the rollback utility to generate `reverse_targeted_rules.csv` and re-apply original legacy rule functions:

```bash
# 1. Generate reverse targeted CSV payload
python revert_mapping_to_old_rules.py

# 2. Re-apply legacy rule functions in Staging DB
python 5_run_bulk_update.py --input-file temp-data/reverse_targeted_rules.csv --do-update

```

### Recovery from Paused Pipeline Ingestion Jobs

If pipeline execution halts after Stage 6 (where download jobs are disabled), re-enable verified ingestion schedules by running Stage 9 directly:

```bash
python 9_verify_and_enable_api.py
```
