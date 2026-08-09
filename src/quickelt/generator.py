
from pathlib import Path
from rich.progress import Progress, SpinnerColumn, TextColumn
from typing import Dict, Any

from .templates import (
    generate_storage,
    generate_customers_contract,
    generate_ingest_customers,
    generate_transform_customers,
    generate_aggregate_customers,
    generate_soda_checks,
    generate_tf_main,
    generate_tf_variables,
    generate_conftest,
    generate_test_pipelines
)

def generate_project(project_path: Path, params: Dict[str, Any]):
    # params contain: cloud_target, engine, data_quality, infra, project_name
    
    # 1. Directory Structure
    dirs = [
        "config",
        "contracts",
        "pipelines/bronze",
        "pipelines/silver",
        "pipelines/gold",
        "quality",
        "infrastructure",
        "tests",
    ]
    
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        transient=True,
    ) as progress:
        task1 = progress.add_task("[cyan]Creating directories...", total=len(dirs))
        
        for d in dirs:
            (project_path / d).mkdir(parents=True, exist_ok=True)
            progress.update(task1, advance=1)
            
        task2 = progress.add_task("[cyan]Generating configuration files...", total=4)
        
        # 2. Config files
        # .env.example
        env_content = generate_env_example(params)
        (project_path / ".env.example").write_text(env_content)
        progress.update(task2, advance=1)
        
        # .gitignore
        (project_path / ".gitignore").write_text(generate_gitignore())
        progress.update(task2, advance=1)
        
        # pyproject.toml
        (project_path / "pyproject.toml").write_text(generate_pyproject(params))
        progress.update(task2, advance=1)
        
        # README.md
        (project_path / "README.md").write_text(generate_readme(params))
        progress.update(task2, advance=1)
        
        task3 = progress.add_task("[cyan]Generating Python modules...", total=2)
        
        # 3. Config modules
        (project_path / "config" / "__init__.py").write_text("")
        progress.update(task3, advance=1)
        
        (project_path / "config" / "settings.py").write_text(generate_settings(params))
        progress.update(task3, advance=1)

        task4 = progress.add_task("[cyan]Generating example code files...", total=10)
        
        # config/storage.py
        (project_path / "config" / "storage.py").write_text(generate_storage(params))
        progress.update(task4, advance=1)
        
        # contracts/customers.py
        (project_path / "contracts" / "__init__.py").write_text("")
        (project_path / "contracts" / "customers.py").write_text(generate_customers_contract())
        progress.update(task4, advance=1)
        
        # pipelines files
        (project_path / "pipelines" / "__init__.py").write_text("")
        (project_path / "pipelines" / "bronze" / "__init__.py").write_text("")
        (project_path / "pipelines" / "bronze" / "ingest_customers.py").write_text(generate_ingest_customers(params))
        progress.update(task4, advance=1)
        
        (project_path / "pipelines" / "silver" / "__init__.py").write_text("")
        (project_path / "pipelines" / "silver" / "transform_customers.py").write_text(generate_transform_customers(params))
        progress.update(task4, advance=1)
        
        (project_path / "pipelines" / "gold" / "__init__.py").write_text("")
        (project_path / "pipelines" / "gold" / "aggregate_customers.py").write_text(generate_aggregate_customers(params))
        progress.update(task4, advance=1)
        
        # quality
        (project_path / "quality" / "soda_checks.yml").write_text(generate_soda_checks())
        progress.update(task4, advance=1)
        
        # infrastructure
        (project_path / "infrastructure" / "main.tf").write_text(generate_tf_main(params))
        (project_path / "infrastructure" / "variables.tf").write_text(generate_tf_variables(params))
        progress.update(task4, advance=1)
        
        # tests
        (project_path / "tests" / "__init__.py").write_text("")
        (project_path / "tests" / "conftest.py").write_text(generate_conftest())
        progress.update(task4, advance=1)
        
        (project_path / "tests" / "test_pipelines.py").write_text(generate_test_pipelines())
        progress.update(task4, advance=2) # advance 2 to complete 10



def generate_env_example(params: Dict[str, Any]) -> str:
    cloud = params.get("cloud_target", "")
    lines = [
        "# Lakehouse Configuration",
        "LAKEHOUSE_ENV=dev",
        "BUCKET_BRONZE=my-bronze-bucket",
        "BUCKET_SILVER=my-silver-bucket",
        "BUCKET_GOLD=my-gold-bucket",
        "",
        "# Cloud Credentials"
    ]
    if cloud == "AWS S3":
        lines.extend([
            "AWS_ACCESS_KEY_ID=your_access_key",
            "AWS_SECRET_ACCESS_KEY=your_secret_key",
            "AWS_REGION=us-east-1"
        ])
    elif cloud == "Local MinIO":
        lines.extend([
            "AWS_ACCESS_KEY_ID=minioadmin",
            "AWS_SECRET_ACCESS_KEY=minioadmin",
            "AWS_REGION=us-east-1",
            "ENDPOINT_URL=http://localhost:9000"
        ])
    elif cloud == "Azure ADLS":
        lines.extend([
            "AZURE_STORAGE_ACCOUNT_NAME=your_account",
            "AZURE_STORAGE_ACCOUNT_KEY=your_key"
        ])
    elif cloud == "GCP GCS":
        lines.extend([
            "GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json"
        ])
    return "\n".join(lines) + "\n"

def generate_gitignore() -> str:
    return """# Python
__pycache__/
*.py[cod]
*$py.class
.venv/
env/
venv/
.env

# Data
*.parquet
*.duckdb
*.csv
*.json

# Testing
.pytest_cache/
.coverage
htmlcov/
"""

def generate_pyproject(params: Dict[str, Any]) -> str:
    project_name = params.get("project_name", "my_lakehouse")
    engine = params.get("engine", "Polars")
    data_quality = params.get("data_quality", "Pandera + Soda Core")
    
    deps = [
        '"pydantic-settings>=2.0.0"',
    ]
    if engine == "Polars":
        deps.append('"polars>=1.0.0"')
        deps.append('"pyarrow>=14.0.0"')
        if params.get("cloud_target") in ["AWS S3", "Local MinIO"]:
            deps.append('"s3fs>=2024.1.0"')
        elif params.get("cloud_target") == "Azure ADLS":
            deps.append('"adlfs>=2024.1.0"')
        elif params.get("cloud_target") == "GCP GCS":
            deps.append('"gcsfs>=2024.1.0"')
    elif engine == "DuckDB":
        deps.append('"duckdb>=1.0.0"')
        
    if "Pandera" in data_quality:
        deps.append('"pandas>=2.0.0"')
        if engine == "Polars":
            deps.append('"pandera[polars]>=0.20.0"')
        else:
            deps.append('"pandera>=0.20.0"')
    if "Soda Core" in data_quality:
        deps.append('"soda-core>=3.0.0"')
    if "Great Expectations" in data_quality:
        deps.append('"great-expectations>=0.18.0"')
        
    deps_str = ",\n    ".join(deps)
    
    return f"""[project]
name = "{project_name}"
version = "0.1.0"
description = "Lakehouse project generated by QuickELT"
readme = "README.md"
requires-python = ">=3.10"
dependencies = [
    {deps_str}
]

[dependency-groups]
dev = [
    "pytest>=8.0.0",
]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["config", "contracts", "pipelines"]
"""

def generate_readme(params: Dict[str, Any]) -> str:
    project_name = params.get("project_name", "my_lakehouse")
    return f"""# {project_name}

Lakehouse project generated by QuickELT.

## Setup

1. Make sure you have `uv` installed.
2. Run `uv sync` to install dependencies.
3. Copy `.env.example` to `.env` and fill in your credentials and bucket names.

## Project Structure

- `config/`: Application configuration settings.
- `contracts/`: Data contracts definition.
- `pipelines/`: Bronze, Silver, and Gold data pipelines.
- `quality/`: Data quality checks.
- `infrastructure/`: Infrastructure as Code.
- `tests/`: Unit and integration tests.

## Running

Run the pipelines using:
```bash
uv run python -m pipelines.bronze.ingest_example
```
"""

def generate_settings(params: Dict[str, Any]) -> str:
    cloud = params.get("cloud_target", "")
    
    fields = [
        'env: str = Field(default="dev", alias="LAKEHOUSE_ENV")',
        'bucket_bronze: str = Field(..., alias="BUCKET_BRONZE")',
        'bucket_silver: str = Field(..., alias="BUCKET_SILVER")',
        'bucket_gold: str = Field(..., alias="BUCKET_GOLD")'
    ]
    
    if cloud in ["AWS S3", "Local MinIO"]:
        fields.extend([
            'aws_access_key_id: str = Field(..., alias="AWS_ACCESS_KEY_ID")',
            'aws_secret_access_key: str = Field(..., alias="AWS_SECRET_ACCESS_KEY")',
            'aws_region: str = Field(default="us-east-1", alias="AWS_REGION")'
        ])
        if cloud == "Local MinIO":
            fields.append('endpoint_url: str = Field(..., alias="ENDPOINT_URL")')
    elif cloud == "Azure ADLS":
        fields.extend([
            'azure_storage_account_name: str = Field(..., alias="AZURE_STORAGE_ACCOUNT_NAME")',
            'azure_storage_account_key: str = Field(..., alias="AZURE_STORAGE_ACCOUNT_KEY")'
        ])
    elif cloud == "GCP GCS":
        fields.extend([
            'google_application_credentials: str = Field(..., alias="GOOGLE_APPLICATION_CREDENTIALS")'
        ])
        
    fields_str = "\n    ".join(fields)
    
    return f"""from pydantic_settings import BaseSettings
from pydantic import Field

class Settings(BaseSettings):
    {fields_str}

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "ignore"

settings = Settings()
"""
