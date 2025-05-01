"""
Testes Unitários para Databases Ingestion Template PRO usando Polars

Este módulo testa individualmente cada função do template de ingestão de bancos de dados com Polars.
Each function from the database ingestion template using Polars is individually tested.

Dependências / Dependencies:
- pytest
- polars
- pandas
- sqlalchemy
- python-dotenv
- os
- json

INSTRUÇÕES:
- Certifique-se de ter um arquivo .env configurado para testes.
- Execute os testes com o comando: pytest test_ingestion_databases_polars_functions.py

INSTRUCTIONS:
- Make sure you have a .env file configured for testing.
- Run tests with the command: pytest test_ingestion_databases_polars_functions.py
"""

import pytest
import os
import json
import polars as pl
import pandas as pd
from sqlalchemy import create_engine
from ingestion.pandas_templates import databases_ingestion_polars as ingestion

@pytest.fixture(scope="module")
def setup_env():
    """
    Fixture para carregar variáveis de ambiente.
    Fixture to load environment variables.
    """
    from dotenv import load_dotenv
    load_dotenv()

# ---------------- Testes -------------------

def test_validate_env_variables(setup_env):
    ingestion.validate_env_variables()


def test_build_connection_string(setup_env):
    connection_string = ingestion.build_connection_string()
    assert isinstance(connection_string, str)
    assert any(driver in connection_string for driver in ["postgresql", "mysql", "oracle", "mssql"])


def test_connect_to_database(setup_env):
    connection_string = ingestion.build_connection_string()
    engine = ingestion.connect_to_database(connection_string)
    assert engine is not None
    assert hasattr(engine, 'connect')


def test_load_data_as_polars(setup_env):
    connection_string = ingestion.build_connection_string()
    engine = ingestion.connect_to_database(connection_string)
    query = os.getenv("DB_QUERY")
    df = ingestion.load_data_as_polars(query=query, engine=engine)
    assert isinstance(df, pl.DataFrame)


def test_generate_file_paths():
    output_data_file, output_metadata_file, nome_arquivo, timestamp = ingestion.generate_file_paths("test", "polars")
    assert output_data_file.endswith("test_polars_" + timestamp)
    assert output_metadata_file.endswith("_metadata.json")


def test_save_polars_dataframe(tmp_path):
    df = pl.DataFrame({"col1": [1, 2], "col2": [3, 4]})
    output_path = tmp_path / "test_output"

    ingestion.save_polars_dataframe(df, str(output_path))
    assert os.path.exists(f"{output_path}.parquet")


def test_generate_metadata(tmp_path):
    df = pl.DataFrame({"col1": [1, 2], "col2": [3, 4]})
    output_file = tmp_path / "datafile"
    output_metadata_file = tmp_path / "metadatafile.json"
    query = "SELECT 1"
    ingestion.generate_metadata(df, query, str(output_file), str(output_metadata_file), "database", "polars", "2025-05-01")

    assert os.path.exists(output_metadata_file)
    with open(output_metadata_file, "r", encoding="utf-8") as f:
        metadata = json.load(f)
        assert metadata["status"] == "success"


def test_ingest_end_to_end(tmp_path, monkeypatch, setup_env):
    """
    Teste de ponta a ponta do processo de ingestão usando Polars.
    End-to-end test of the ingestion process using Polars.
    """
    monkeypatch.setattr(ingestion, "BRONZE_PATH", str(tmp_path) + "/bronze/")
    monkeypatch.setattr(ingestion, "generate_file_paths", lambda origem, formato: (str(tmp_path / "bronze" / "test_output"), str(tmp_path / "metadata" / "test_metadata.json"), "test", "2025-05-01"))

    ingestion.ingest()

    # Verifica se arquivos foram criados
    assert any(f.endswith(".parquet") for f in os.listdir(tmp_path / "bronze"))
    assert any(f.endswith(".json") for f in os.listdir(tmp_path / "metadata"))