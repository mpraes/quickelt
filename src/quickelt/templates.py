from typing import Dict, Any

def generate_storage(params: Dict[str, Any]) -> str:
    engine = params.get("engine", "Polars")
    return f"""import os
from config.settings import settings

def get_base_path(layer: str) -> str:
    \"\"\"Return the base URI for a given data layer.\"\"\"
    bucket = getattr(settings, f"bucket_{{layer}}")
    
    if os.environ.get("USE_LOCAL_STORAGE") == "1":
        base = os.environ.get("LOCAL_STORAGE_PATH", "/tmp/lakehouse")
        return f"{{base}}/{{layer}}"
        
    cloud = "{params.get('cloud_target', '')}"
    if "S3" in cloud or "MinIO" in cloud:
        return f"s3://{{bucket}}"
    elif "Azure" in cloud:
        return f"abfs://{{bucket}}"
    elif "GCP" in cloud:
        return f"gs://{{bucket}}"
        
    return f"{{bucket}}"
    
def get_table_path(layer: str, table_name: str) -> str:
    return f"{{get_base_path(layer)}}/{{table_name}}.parquet"
"""

def generate_customers_contract() -> str:
    return """import pandera as pa
from pandera.typing import Series

class CustomerSchema(pa.DataFrameModel):
    customer_id: Series[int] = pa.Field(ge=1)
    email: Series[str] = pa.Field(nullable=False)
    created_at: Series[pa.DateTime]
    
    class Config:
        coerce = True
"""

def generate_ingest_customers(params: Dict[str, Any]) -> str:
    engine = params.get("engine", "Polars")
    
    if engine == "DuckDB":
        return """import duckdb
from config.storage import get_table_path

def ingest_customers():
    print("Ingesting customers to Bronze layer...")
    bronze_path = get_table_path("bronze", "customers")
    
    conn = duckdb.connect()
    conn.execute(f\"\"\"
        COPY (
            SELECT 
                1 AS customer_id, 
                'alice@example.com' AS email, 
                CAST('2023-01-01' AS TIMESTAMP) AS created_at
            UNION ALL
            SELECT 
                2, 
                'bob@example.com', 
                CAST('2023-01-02' AS TIMESTAMP)
        ) TO '{bronze_path}' (FORMAT 'parquet');
    \"\"\")
    print(f"Data saved to {bronze_path}")

if __name__ == "__main__":
    ingest_customers()
"""
    else:
        return """import polars as pl
from config.storage import get_table_path
from datetime import datetime

def ingest_customers():
    print("Ingesting customers to Bronze layer...")
    bronze_path = get_table_path("bronze", "customers")
    
    df = pl.DataFrame({
        "customer_id": [1, 2],
        "email": ["alice@example.com", "bob@example.com"],
        "created_at": [datetime(2023, 1, 1), datetime(2023, 1, 2)]
    })
    
    # write to parquet
    df.write_parquet(bronze_path)
    print(f"Data saved to {bronze_path}")

if __name__ == "__main__":
    ingest_customers()
"""

def generate_transform_customers(params: Dict[str, Any]) -> str:
    engine = params.get("engine", "Polars")
    
    if engine == "DuckDB":
        return """import duckdb
from config.storage import get_table_path

def transform_customers():
    print("Transforming customers to Silver layer...")
    bronze_path = get_table_path("bronze", "customers")
    silver_path = get_table_path("silver", "customers")
    
    conn = duckdb.connect()
    # Simple clean up - ensuring emails are lowercase
    conn.execute(f\"\"\"
        COPY (
            SELECT 
                customer_id, 
                lower(email) as email, 
                created_at 
            FROM '{bronze_path}'
            WHERE email IS NOT NULL
        ) TO '{silver_path}' (FORMAT 'parquet');
    \"\"\")
    print(f"Data saved to {silver_path}")

if __name__ == "__main__":
    transform_customers()
"""
    else:
        return """import polars as pl
from config.storage import get_table_path
from contracts.customers import CustomerSchema

def transform_customers():
    print("Transforming customers to Silver layer...")
    bronze_path = get_table_path("bronze", "customers")
    silver_path = get_table_path("silver", "customers")
    
    df = pl.read_parquet(bronze_path)
    
    # Simple clean up
    df = df.with_columns(
        pl.col("email").str.to_lowercase()
    ).drop_nulls(subset=["email"])
    
    # Validate contract
    validated_df = CustomerSchema.validate(df.to_pandas())
    
    # Save back to polars and write
    pl.from_pandas(validated_df).write_parquet(silver_path)
    print(f"Data saved to {silver_path}")

if __name__ == "__main__":
    transform_customers()
"""

def generate_aggregate_customers(params: Dict[str, Any]) -> str:
    engine = params.get("engine", "Polars")
    
    if engine == "DuckDB":
        return """import duckdb
from config.storage import get_table_path

def aggregate_customers():
    print("Aggregating customers to Gold layer...")
    silver_path = get_table_path("silver", "customers")
    gold_path = get_table_path("gold", "daily_customers")
    
    conn = duckdb.connect()
    conn.execute(f\"\"\"
        COPY (
            SELECT 
                CAST(created_at AS DATE) as signup_date,
                count(customer_id) as total_customers
            FROM '{silver_path}'
            GROUP BY CAST(created_at AS DATE)
        ) TO '{gold_path}' (FORMAT 'parquet');
    \"\"\")
    print(f"Data saved to {gold_path}")

if __name__ == "__main__":
    aggregate_customers()
"""
    else:
        return """import polars as pl
from config.storage import get_table_path

def aggregate_customers():
    print("Aggregating customers to Gold layer...")
    silver_path = get_table_path("silver", "customers")
    gold_path = get_table_path("gold", "daily_customers")
    
    df = pl.read_parquet(silver_path)
    
    agg_df = (
        df.with_columns(pl.col("created_at").dt.date().alias("signup_date"))
        .group_by("signup_date")
        .agg(pl.len().alias("total_customers"))
    )
    
    agg_df.write_parquet(gold_path)
    print(f"Data saved to {gold_path}")

if __name__ == "__main__":
    aggregate_customers()
"""

def generate_soda_checks() -> str:
    return """# Soda Core Quality Checks for Silver Layer
checks for customers:
  - row_count > 0
  - missing_count(customer_id) = 0
  - duplicate_count(customer_id) = 0
  - missing_count(email) = 0
"""

def generate_tf_main(params: Dict[str, Any]) -> str:
    cloud = params.get("cloud_target", "")
    if "AWS" in cloud or "MinIO" in cloud:
        return """provider "aws" {
  region = var.aws_region
}

resource "aws_s3_bucket" "bronze" {
  bucket = var.bucket_bronze
}

resource "aws_s3_bucket" "silver" {
  bucket = var.bucket_silver
}

resource "aws_s3_bucket" "gold" {
  bucket = var.bucket_gold
}
"""
    elif "Azure" in cloud:
        return """provider "azurerm" {
  features {}
}

resource "azurerm_storage_account" "lake" {
  name                     = var.storage_account_name
  resource_group_name      = var.resource_group_name
  location                 = var.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
}

resource "azurerm_storage_container" "bronze" {
  name                  = var.bucket_bronze
  storage_account_name  = azurerm_storage_account.lake.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "silver" {
  name                  = var.bucket_silver
  storage_account_name  = azurerm_storage_account.lake.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "gold" {
  name                  = var.bucket_gold
  storage_account_name  = azurerm_storage_account.lake.name
  container_access_type = "private"
}
"""
    elif "GCP" in cloud:
        return """provider "google" {
  project = var.gcp_project
  region  = var.gcp_region
}

resource "google_storage_bucket" "bronze" {
  name     = var.bucket_bronze
  location = var.gcp_region
}

resource "google_storage_bucket" "silver" {
  name     = var.bucket_silver
  location = var.gcp_region
}

resource "google_storage_bucket" "gold" {
  name     = var.bucket_gold
  location = var.gcp_region
}
"""
    return ""

def generate_tf_variables(params: Dict[str, Any]) -> str:
    cloud = params.get("cloud_target", "")
    base = """variable "bucket_bronze" { type = string }
variable "bucket_silver" { type = string }
variable "bucket_gold" { type = string }
"""
    if "AWS" in cloud or "MinIO" in cloud:
        return base + '\nvariable "aws_region" { type = string, default = "us-east-1" }\n'
    elif "Azure" in cloud:
        return base + '\nvariable "storage_account_name" { type = string }\nvariable "resource_group_name" { type = string }\nvariable "location" { type = string }\n'
    elif "GCP" in cloud:
        return base + '\nvariable "gcp_project" { type = string }\nvariable "gcp_region" { type = string }\n'
    return base

def generate_conftest() -> str:
    return """import os
import pytest

@pytest.fixture(autouse=True)
def setup_local_storage(tmp_path):
    \"\"\"Setup local storage paths for tests without mocking cloud.\"\"\"
    os.environ["USE_LOCAL_STORAGE"] = "1"
    os.environ["LOCAL_STORAGE_PATH"] = str(tmp_path)
    
    (tmp_path / "bronze").mkdir()
    (tmp_path / "silver").mkdir()
    (tmp_path / "gold").mkdir()
    
    yield
    
    del os.environ["USE_LOCAL_STORAGE"]
    del os.environ["LOCAL_STORAGE_PATH"]
"""

def generate_test_pipelines() -> str:
    return """import os
from pipelines.bronze.ingest_customers import ingest_customers
from pipelines.silver.transform_customers import transform_customers
from pipelines.gold.aggregate_customers import aggregate_customers
from config.storage import get_table_path

def test_full_pipeline_execution():
    # 1. Ingest
    ingest_customers()
    assert os.path.exists(get_table_path("bronze", "customers"))
    
    # 2. Transform
    transform_customers()
    assert os.path.exists(get_table_path("silver", "customers"))
    
    # 3. Aggregate
    aggregate_customers()
    assert os.path.exists(get_table_path("gold", "daily_customers"))
"""
