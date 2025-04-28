"""
API Ingestion Template using Pandas

Este é um template profissional para consumir dados de uma API pública ou privada usando Pandas,
armazenar os dados no diretório bronze, e gerar automaticamente um arquivo de metadados.

ORIENTAÇÕES:
- Defina a URL e Token da API no arquivo .env.
- Faça a requisição e carregue o JSON.
- Converta para DataFrame Pandas.
- Salve o resultado como CSV.
- Gere também um arquivo de metadados (.json) organizado por data.

INSTRUCTIONS:
- Set API URL and Token in .env.
- Make the request and load JSON response.
- Convert to Pandas DataFrame.
- Save as CSV in bronze directory.
- Generate metadata file (.json) organized by date.

Dependências / Dependencies:
- pandas
- requests
- python-dotenv
"""

import os
import requests
import pandas as pd
import json
from datetime import datetime
from dotenv import load_dotenv

from utils.logger import setup_logger

# Setup
logger = setup_logger("api_ingestion_pandas_template")
load_dotenv()

# Constantes
BRONZE_PATH = "./data/bronze/"

def generate_file_paths(origem: str, formato: str) -> tuple:
    """
    Gera os caminhos para salvar o arquivo de dados e o arquivo de metadados.
    """
    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    nome_arquivo = f"{origem}_{formato}_{timestamp}"

    # Diretórios
    output_data_file = os.path.join(BRONZE_PATH, f"{nome_arquivo}.csv")

    today = datetime.now()
    metadata_dir = os.path.join("metadata", str(today.year), f"{today.month:02d}", f"{today.day:02d}")
    os.makedirs(metadata_dir, exist_ok=True)
    output_metadata_file = os.path.join(metadata_dir, f"{nome_arquivo}_metadata.json")

    return output_data_file, output_metadata_file, nome_arquivo, timestamp

def ingest():
    """
    Função principal para realizar a ingestão da API e salvar dados + metadados.
    Main function to ingest API data and save data + metadata.
    """

    origem = "api"
    formato = "pandas"

    url = os.getenv("API_URL")
    token = os.getenv("API_TOKEN")

    if not url:
        logger.error("API_URL não definida no .env / API_URL not defined in .env")
        return

    headers = {"Authorization": f"Bearer {token}"} if token else {}

    try:
        logger.info(f"Enviando requisição para API: {url} / Sending request to API: {url}")
        response = requests.get(url, headers=headers)
        response.raise_for_status()

        data = response.json()

        # Transformar JSON em DataFrame
        if isinstance(data, list):
            df = pd.DataFrame(data)
        elif isinstance(data, dict):
            df = pd.DataFrame(data.get("results", []))
        else:
            logger.error("Formato de resposta da API não reconhecido / Unrecognized API response format")
            return

        logger.info(f"DataFrame Pandas criado com {df.shape[0]} linhas e {df.shape[1]} colunas / Pandas DataFrame created with {df.shape[0]} rows and {df.shape[1]} columns")

        os.makedirs(BRONZE_PATH, exist_ok=True)

        output_data_file, output_metadata_file, nome_arquivo, timestamp = generate_file_paths(origem, formato)

        df.to_csv(output_data_file, index=False)

        logger.info(f"Dados salvos com sucesso em {output_data_file} / Data successfully saved to {output_data_file}")

        # Gerar metadados
        metadata = {
            "origem": origem,
            "formato": formato,
            "timestamp": timestamp,
            "status": "success",
            "output_file": output_data_file,
            "quantidade_linhas": df.shape[0],
            "quantidade_colunas": df.shape[1]
        }

        with open(output_metadata_file, "w", encoding="utf-8") as f:
            json.dump(metadata, f, ensure_ascii=False, indent=4)

        logger.info(f"Metadados salvos em {output_metadata_file} / Metadata saved in {output_metadata_file}")

    except Exception as e:
        logger.error(f"Erro durante a ingestão da API: {str(e)} / Error during API ingestion: {str(e)}")

if __name__ == "__main__":
    ingest()
