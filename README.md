# QuickELT

QuickELT is a data engineering starter project focused on practical ingestion templates and reproducible infrastructure setup.

## What You Get

- Data ingestion templates for common sources.
- Local-first development flow with Python tooling (`uv`, `pytest`).
- Infrastructure setup wizard with named setup workspaces.
- Azure Terraform provisioning and destroy workflow.
- Automated infrastructure test suite.

## Requirements

- Python 3.10+
- `uv` for dependency management
- Cloud CLIs depending on your provider:
  - AWS CLI for AWS flows
  - Azure CLI for Azure flows
- Terraform (auto-install is supported by the setup wizard on Linux/macOS)

## Installation

```bash
git clone https://github.com/mpraes/quickelt.git
cd quickelt
uv sync
```

## Quick Start

Use the CLI entrypoint:

```bash
quickelt setup
```

This launches the interactive infrastructure setup wizard and creates a named setup under `infrastructure/setups/<setup-name>/`.

## CLI Usage

```bash
quickelt setup
quickelt setup --destroy --setup-name my-project
quickelt cleanup --setup-name my-project
quickelt cleanup --setup-name my-project --yes
```

Behavior summary:

- `setup`: creates or configures infrastructure for a named setup.
- `setup --destroy`: destroys Azure Terraform resources tied to the setup workspace.
- `cleanup`: removes local setup files and Terraform state only (no cloud destroy).

## Run Tests

```bash
pytest infrastructure/test -q
```

## Validate Terraform Module

```bash
cd infrastructure/terraform/azure
terraform fmt -check -recursive
terraform init -backend=false
terraform validate
```

## Project Structure

```text
quickelt/
├── docs/
├── infrastructure/
│   ├── setup.py
│   ├── setup/
│   ├── terraform/azure/
│   └── test/
├── src/
├── pyproject.toml
└── README.md
```

## Documentation

- Infrastructure execution details: `docs/INFRASTRUCTURE_GUIDE-EN.md`
- Ingestion considerations: `docs/INGESTION_MAIN_CONSIDERATIONS.md`
- Project checklist: `docs/CHECKLIST.md`

## Contributing

1. Create a feature branch.
2. Make changes with tests.
3. Run `pytest infrastructure/test -q`.
4. Open a pull request with a clear summary.
