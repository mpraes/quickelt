"""
CSV Ingestion Template using Polars

Este é um template profissional para carregar arquivos CSV locais utilizando Polars,
armazenar os dados no diretório bronze e gerar automaticamente um arquivo de metadados.

ORIENTAÇÕES:
- Defina o caminho de entrada do CSV no .env.
- Carregue o CSV usando Polars.
- Salve o arquivo como Parquet no bronze.
- Gere um arquivo de metadados (.json) organizado por data.

INSTRUCTIONS:
- Set the input CSV path in the .env file.
- Load the CSV using Polars.
- Save as Parquet into bronze directory.
- Generate metadata file (.json) organized by date.

Dependências / Dependencies:
- polars
- python-dotenv
"""

import os
import polars as pl
import json
from datetime import datetime
from dotenv import load_dotenv

from utils.logger import setup_logger

# Setup
logger = setup_logger("csv_ingestion_polars_template")
load_dotenv()

# Constantes
BRONZE_PATH = "./data/bronze/"

def generate_file_paths(origem: str, formato: str) -> tuple:
    """
    Gera os caminhos para salvar o arquivo de dados e o arquivo de metadados.
    """
    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    nome_arquivo = f"{origem}_{formato}_{timestamp}"

    output_data_file = os.path.join(BRONZE_PATH, f"{nome_arquivo}.parquet")

    today = datetime.now()
    metadata_dir = os.path.join("metadata", str(today.year), f"{today.month:02d}", f"{today.day:02d}")
    os.makedirs(metadata_dir, exist_ok=True)
    output_metadata_file = os.path.join(metadata_dir, f"{nome_arquivo}_metadata.json")

    return output_data_file, output_metadata_file, nome_arquivo, timestamp

def ingest():
    """
    Função principal para realizar a ingestão do CSV e salvar dados + metadados usando Polars.
    Main function to ingest CSV data and save data + metadata using Polars.
    """

    origem = "csv"
    formato = "polars"

    input_csv_path = os.getenv("INPUT_CSV_PATH")

    if not input_csv_path:
        logger.error("INPUT_CSV_PATH não definido no .env / INPUT_CSV_PATH not defined in .env")
        return

    if not os.path.exists(input_csv_path):
        logger.error(f"Arquivo CSV não encontrado: {input_csv_path} / CSV file not found: {input_csv_path}")
        return

    try:
        logger.info(f"Carregando arquivo CSV: {input_csv_path} / Loading CSV file: {input_csv_path}")
        df = pl.read_csv(input_csv_path)

        logger.info(f"DataFrame Polars carregado com {df.shape[0]} linhas e {df.shape[1]} colunas / Polars DataFrame loaded with {df.shape[0]} rows and {df.shape[1]} columns")

        os.makedirs(BRONZE_PATH, exist_ok=True)

        output_data_file, output_metadata_file, nome_arquivo, timestamp = generate_file_paths(origem, formato)

        df.write_parquet(output_data_file)

        logger.info(f"Dados salvos com sucesso em {output_data_file} (Parquet) / Data successfully saved to {output_data_file} (Parquet)")

        # Gerar metadados
        metadata = {
            "origem": origem,
            "formato": formato,
            "timestamp": timestamp,
            "status": "success",
            "input_file": input_csv_path,
            "output_file": output_data_file,
            "quantidade_linhas": df.shape[0],
            "quantidade_colunas": df.shape[1]
        }

        with open(output_metadata_file, "w", encoding="utf-8") as f:
            json.dump(metadata, f, ensure_ascii=False, indent=4)

        logger.info(f"Metadados salvos em {output_metadata_file} / Metadata saved in {output_metadata_file}")

    except Exception as e:
        logger.error(f"Erro durante a ingestão do CSV: {str(e)} / Error during CSV ingestion: {str(e)}")

if __name__ == "__main__":
    ingest()
