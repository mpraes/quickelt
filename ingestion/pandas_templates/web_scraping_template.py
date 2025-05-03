"""
Web Scraping Template using Pandas

(Seções de orientações, instruções, fluxo de execução e dependências já foram adicionadas acima)
"""

import os
import requests
import pandas as pd
import json
from bs4 import BeautifulSoup
from datetime import datetime
from dotenv import load_dotenv

from utils.logger import setup_logger
from utils.pydantic_validation import validate_with_pydantic_batch
from contracts.data_contracts import ProductWebScrapingContract  # Ajuste conforme seu contrato real

# Setup
logger = setup_logger("web_scraping_pandas_template")
load_dotenv()

# Constantes
BRONZE_PATH = "./data/bronze/"

def generate_file_paths(origem: str, formato: str) -> tuple:
    """
    Gera os caminhos para salvar o arquivo de dados e o arquivo de metadados.
    Generate the paths to save the data file and the metadata file.
    """
    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    nome_arquivo = f"{origem}_{formato}_{timestamp}"

    output_data_file = os.path.join(BRONZE_PATH, f"{nome_arquivo}.csv")

    today = datetime.now()
    metadata_dir = os.path.join("metadata", str(today.year), f"{today.month:02d}", f"{today.day:02d}")
    os.makedirs(metadata_dir, exist_ok=True)
    output_metadata_file = os.path.join(metadata_dir, f"{nome_arquivo}_metadata.json")

    return output_data_file, output_metadata_file, nome_arquivo, timestamp

def ingest():
    """
    Função principal para realizar Web Scraping e salvar dados + metadados usando Pandas.
    Main function to perform Web Scraping and save data + metadata using Pandas.
    """
    origem = "webscraping"
    formato = "pandas"

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
            logger.info("Tabela HTML encontrada, convertendo para DataFrame Pandas / HTML table found, converting to Pandas DataFrame")
            df = pd.read_html(str(table))[0]

            logger.info(f"DataFrame Pandas carregado com {df.shape[0]} linhas e {df.shape[1]} colunas / Pandas DataFrame loaded with {df.shape[0]} rows and {df.shape[1]} columns")

            # Validação Pydantic
            try:
                validated_df = validate_with_pydantic_batch(df, ProductWebScrapingContract)
                logger.info("Validação Pydantic concluída com sucesso / Pydantic validation completed successfully")
            except Exception as e:
                logger.error(f"Erro de validação Pydantic: {str(e)} / Pydantic validation error: {str(e)}")
                return

            os.makedirs(BRONZE_PATH, exist_ok=True)

            output_data_file, output_metadata_file, nome_arquivo, timestamp = generate_file_paths(origem, formato)

            validated_df.to_csv(output_data_file, index=False)
            logger.info(f"Dados salvos com sucesso em {output_data_file} / Data successfully saved to {output_data_file}")

            # Gerar metadados
            metadata = {
                "origem": origem,
                "formato": formato,
                "timestamp": timestamp,
                "status": "success",
                "scraping_url": url,
                "output_file": output_data_file,
                "quantidade_linhas": validated_df.shape[0],
                "quantidade_colunas": validated_df.shape[1],
                "colunas_tipos": validated_df.dtypes.astype(str).to_dict()
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
