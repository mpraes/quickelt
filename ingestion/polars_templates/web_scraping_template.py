"""
Web Scraping Template using Polars

Este é um template profissional para realizar web scraping de sites,
utilizando Polars para grandes volumes, armazenar os dados no diretório bronze
e gerar automaticamente um arquivo de metadados.

ORIENTAÇÕES:
- Defina a URL alvo e o User-Agent no .env.
- Faça a requisição ao site usando requests.
- Extraia a tabela desejada usando Pandas, depois converta para Polars.
- Salve como Parquet.
- Gere um arquivo de metadados (.json) organizado por data.

INSTRUCTIONS:
- Set the target URL and User-Agent in .env.
- Request the site using requests.
- Extract desired table using Pandas, then convert to Polars.
- Save as Parquet into bronze directory.
- Generate metadata file (.json) organized by date.

Dependências / Dependencies:
- polars
- pandas
- requests
- beautifulsoup4
- python-dotenv
"""

import os
import requests
import pandas as pd
import polars as pl
import io
import json
from bs4 import BeautifulSoup
from datetime import datetime
from dotenv import load_dotenv

from utils.logger import setup_logger

# Setup
logger = setup_logger("web_scraping_polars_template")
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
    Função principal para realizar Web Scraping e salvar dados + metadados usando Polars.
    Main function to perform Web Scraping and save data + metadata using Polars.
    """

    origem = "webscraping"
    formato = "polars"

    url = os.getenv("SCRAPING_URL")
    user_agent = os.getenv("SCRAPING_USER_AGENT")

    if not url:
        logger.error("SCRAPING_URL não definida no .env / SCRAPING_URL not defined in .env")
        return

    headers = {"User-Agent": user_agent} if user_agent else {}

    try:
        logger.info(f"Enviando requisição para {url} / Sending request to {url}")
        response = requests.get(url, headers=headers)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")

        table = soup.find("table")

        if table:
            logger.info("Tabela HTML encontrada, convertendo para DataFrame Polars / HTML table found, converting to Polars DataFrame")
            df_pandas = pd.read_html(str(table))[0]
            df_polars = pl.DataFrame(df_pandas)

            logger.info(f"DataFrame Polars carregado com {df_polars.shape[0]} linhas e {df_polars.shape[1]} colunas / Polars DataFrame loaded with {df_polars.shape[0]} rows and {df_polars.shape[1]} columns")

            os.makedirs(BRONZE_PATH, exist_ok=True)

            output_data_file, output_metadata_file, nome_arquivo, timestamp = generate_file_paths(origem, formato)

            df_polars.write_parquet(output_data_file)

            logger.info(f"Dados salvos com sucesso em {output_data_file} (Parquet) / Data successfully saved to {output_data_file} (Parquet)")

            # Gerar metadados
            metadata = {
                "origem": origem,
                "formato": formato,
                "timestamp": timestamp,
                "status": "success",
                "scraping_url": url,
                "output_file": output_data_file,
                "quantidade_linhas": df_polars.shape[0],
                "quantidade_colunas": df_polars.shape[1]
            }

            with open(output_metadata_file, "w", encoding="utf-8") as f:
                json.dump(metadata, f, ensure_ascii=False, indent=4)

            logger.info(f"Metadados salvos em {output_metadata_file} / Metadata saved in {output_metadata_file}")

        else:
            logger.error("Nenhuma tabela encontrada na página / No table found on the page")

    except Exception as e:
        logger.error(f"Erro durante o Web Scraping: {str(e)} / Error during Web Scraping: {str(e)}")

if __name__ == "__main__":
    ingest()
