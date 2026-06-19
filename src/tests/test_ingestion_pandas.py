"""
Testes Automáticos para Templates de Ingestão usando Pandas

Este módulo contém testes para validar se os templates de ingestão que utilizam Pandas estão funcionando corretamente.

ORIENTAÇÕES:
- Cada teste importa o respectivo módulo de ingestão e executa a função ingest().
- O teste verifica se:
  - Um arquivo de dados (.csv) foi gerado no diretório bronze.
  - Um arquivo de metadados (.json) foi gerado no diretório metadata.
  - (Opcional) O arquivo de metadados é um JSON válido.
- Os arquivos são nomeados automaticamente com origem, formato e timestamp.

INSTRUCTIONS:
- Each test imports the respective ingestion module and executes the ingest() function.
- The test checks:
  - A data file (.csv) was generated in the bronze directory.
  - A metadata file (.json) was generated in the metadata directory.
  - (Optional) The metadata file is valid JSON.
- Files are automatically named with source, format, and timestamp.

Dependências / Dependencies:
- pytest
- os
- json
"""


import pytest
import os
import json

def run_test_ingestion(module_import_path, expected_data_suffix):
    ingestion_module = __import__(module_import_path, fromlist=["ingest"])
    ingestion_module.ingest()

    bronze_files = os.listdir("./data/bronze")
    metadata_files = []

    for root, dirs, files in os.walk("./metadata"):
        metadata_files.extend(files)

    # Verifica se algum arquivo de dados esperado foi gerado
    assert any(f.endswith(expected_data_suffix) for f in bronze_files), "Arquivo de dados não encontrado / Data file not found"
    
    # Verifica se algum metadado foi gerado
    assert any(f.endswith("_metadata.json") for f in metadata_files), "Arquivo de metadados não encontrado / Metadata file not found"

    # (Opcional) Verifica se o JSON é válido
    for meta_file in metadata_files:
        with open(os.path.join(root, meta_file), "r", encoding="utf-8") as f:
            json.load(f)  # Lança exceção se inválido

# Agora cada teste usa a função padrão
def test_api_ingestion_pandas():
    run_test_ingestion("ingestion.pandas_templates.api_template", ".csv")

def test_csv_ingestion_pandas():
    run_test_ingestion("ingestion.pandas_templates.csv_template", ".csv")

def test_database_ingestion_pandas():
    run_test_ingestion("ingestion.pandas_templates.databases_template", ".csv")

def test_sharepoint_ingestion_pandas():
    run_test_ingestion("ingestion.pandas_templates.sharepoint_template", ".csv")

def test_web_scraping_ingestion_pandas():
    run_test_ingestion("ingestion.pandas_templates.web_scraping_template", ".csv")
