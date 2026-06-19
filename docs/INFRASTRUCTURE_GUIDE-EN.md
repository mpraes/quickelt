# Quickelt: Automated Infrastructure Architecture Guide
## What happens behind the scenes when you run the CLI?

This document serves as a visual and technical guide so that the Data Engineer understands exactly which resources are touched, validated, and created in the Cloud when running the **Quickelt** interactive assistant.

---

## 🗺️ Execution Flow and Decision Making

When you start the `setup.py` script in your terminal, the tool executes a linear flow divided into 6 steps:

```
[1. Setup Name] ──> [2. Cloud + Pre-flight] ──> [3. Lake] ──> [4. Compute + DW] ──> [5. Provisioning] ──> [6. Delivery (.env)]
```

### CLI commands and setup lifecycle

Quickelt now supports named setup workspaces under `infrastructure/setups/<setup-name>/`.

* `python infrastructure/setup.py` starts a new setup and writes its `.env` inside the named workspace.
* `python infrastructure/setup.py --destroy --setup-name <name>` destroys Azure resources tracked by Terraform state in that setup workspace.
* `python infrastructure/setup.py --clean --setup-name <name>` removes local setup files/state only (no cloud destroy).

The project root `.env` remains the active profile mirror. It is updated to point to whichever setup you last activated.

### Step 1: Context Validation and Authentication
Before requesting any data, the CLI ensures that the local environment is secure and has the necessary dependencies to interact with the chosen provider.

> **Azure Prerequisite:** You must have an active Azure subscription before running the setup. If you don't have one, create a free subscription at [https://azure.microsoft.com/free](https://azure.microsoft.com/free) before proceeding. The `az login` command requires an existing subscription to authenticate successfully.

* **OS Identification:** The script detects whether you are on Linux, macOS, or Windows.
* **CLI Auto-Install:** If the required CLI tool (`aws` or `az`) is not found, the wizard prompts to install it automatically and re-runs the pre-flight check.
* **Credential Check:** * **AWS:** Implicitly runs `aws sts get-caller-identity`. If it fails, it warns that the token has expired or that `aws configure` needs to be run.
    * **Azure:** Runs `az account show`. If the terminal is not logged in, it instructs the immediate use of `az login`.

---

### Step 2: Storage Provisioning (Data Lakehouse)
Quickelt's premise is that no transformation occurs before the data lands in the initial layer. The CLI automates the creation of your data repository.

#### If you choose to create a new Lake:
The script invokes native infrastructure tools to provision low-cost, high-performance storage, immediately structuring the folder ecosystem.

* **On AWS (S3):**
    1. Runs: `aws s3api create-bucket --bucket <lake-name> --region <region>`
    2. Blocks public access to ensure security compliance (LGPD/GDPR).
    3. Creates object delimiters (virtual folders) simulating the Lakehouse pattern:
        * `<lake-name>/bronze/` (Raw data / Landing Zone)
        * `<lake-name>/silver/` (Clean and enriched data in Parquet/Avro)
        * `<lake-name>/gold/` (Analytics layer / Consumption)

* **On Azure (ADLS Gen2 / Blob Storage):**
    1. Creates/validates a Resource Group (`az group exists` / `az group create`).
    2. Provisions via Terraform (default path) and enables *Hierarchical Namespace*.
    3. Creates the main Container and injects the `/bronze`, `/silver`, and `/gold` directory structure.

---

### Step 3: Compute Layer Configuration
Depending on where you chose Quickelt's processing engine to run, the infrastructure behavior changes transparently:

1.  **Local Machine:** No compute infrastructure is created in the cloud. The CLI assumes you will run Python scripts locally or in your own Docker containers.
2.  **Dedicated VM (Processing Server):**
    * **AWS (EC2):** Provisions an instance (default `t3.medium`) running Ubuntu Server via `aws ec2 run-instances`. Fires a *UserData* script at machine startup that updates packages and installs `python3-pip` and `git` automatically.
    * **Azure (VM):** Creates a Linux virtual machine via Terraform. The generated SSH private key is stored locally in the setup Terraform workspace (with `0600` permissions) and its path is exported to `.env` as `AZURE_VM_SSH_PRIVATE_KEY_PATH`.
3.  **Serverless / PaaS (Auto Scaling):**
    * Prepares the environment mappings and permissions (IAM) required so that services like AWS Lambda/Glue or Azure Functions can read the `bronze` layer and write to `silver/gold` without exposing public keys.

---

### Step 4: Data Warehouse Provisioning (Optional)
If you selected an external Gold database, Quickelt offers two strategies:

1. **Local PostgreSQL inside the VM** — Installs PostgreSQL inside the provisioned EC2/VM, creates a `quickelt` user with a cryptographically secure random password (`secrets.token_urlsafe`), and sets up the `quickelt_db` database automatically.
2. **Managed Cloud Service** — Provisions a managed PostgreSQL cluster:
    * **AWS:** Aurora PostgreSQL cluster via `aws rds create-db-cluster` with a primary instance.
    * **Azure:** Azure DB for PostgreSQL Flexible Server via `az postgres flexible-server create`.
    * If you choose to connect to an existing cluster, the wizard collects host, port, username, and password interactively.

All generated passwords use `secrets.token_urlsafe(32)` — never hardcoded defaults. When a managed cluster already exists, the wizard reuses it (with a maximum of 3 retry attempts for name conflicts).

---

### Step 5: State Persistence (.env)
The CLI's final action is purely local. All decisions made by you, paths created, and generated instance IDs are consolidated into a `.env` file at the root of your project.

**Important:** The `.env` writer performs a **merge** — it updates only the wizard-managed keys while preserving any custom variables you may have added manually. Existing wizard keys are updated in-place, and new keys are appended. Comments and custom entries are never deleted.

When the Gold layer uses an external database, the `.env` file permissions are restricted to `0o600` (owner-only read/write) to protect credentials.

This means your infrastructure is now documented by variables. If you need to destroy or recreate the environment, Quickelt will use this file as a static map.

---

## 🔒 Security and Best Practices Applied

* **Principle of Least Privilege:** All creations use the context of the user previously logged into the cloud CLI, respecting existing IAM policies.
* **No Hardcoded Secrets:** All passwords are generated at runtime using `secrets.token_urlsafe()` — the codebase never contains default credentials.
* **.env File Protection:** When database credentials are present, the `.env` file is automatically set to `0o600` permissions (owner-only).
* **Merge-Only Writes:** The `.env` writer preserves user-custom entries and comments, only updating wizard-managed keys.
* **Retry Protection:** Name-conflict handling is capped at 3 retries to prevent infinite recursion.
* **Optimized Cost:** Storages are configured by default without excessive versioning or unnecessary global redundancies for the initial development phase, saving operational costs in the intermediate layer.
