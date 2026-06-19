"""
Web Scraping Template using Polars

Este é um template profissional para realizar scraping de páginas web,
armazenar os dados extraídos no diretório bronze e gerar automaticamente um arquivo de metadados.

ORIENTAÇÕES:
- Defina a URL alvo e as regras de extração no arquivo .env ou diretamente no código.
- Realize o scraping e transforme os dados em um DataFrame Polars.
- Valide usando contratos Pydantic (Data Contracts).
- Salve o resultado como CSV.
- Gere também um arquivo de metadados (.json) organizado por data.

Obs: Para construir um bom sistema de ingestão de dados, consulte o arquivo INGESTION_MAIN_CONSIDERATIONS.md.

INSTRUCTIONS:
- Set the target URL and extraction rules in the .env file or directly in the code.
- Perform the scraping and transform the data into a Polars DataFrame.
- Validate using Pydantic Data Contracts.
- Save as CSV in bronze directory.
- Generate metadata file (.json) organized by date.

Ps: To build a good data ingestion system, consult the INGESTION_MAIN_CONSIDERATIONS.md file.

Fluxo de Execução / Execution Flow:
-----------------------------------

[PT-BR]
1. A função `scrape_webpage(url)` é chamada para extrair dados da página web e carregar como um DataFrame Polars.
2. O DataFrame carregado é passado para a função `validate_dataframe(df)`, que valida os dados utilizando um modelo Pydantic.
3. Se a validação for bem-sucedida, o DataFrame validado é enviado para a função `save_data_and_metadata(df, origin, framework)`.
4. A função `save_data_and_metadata` salva o DataFrame em formato CSV no diretório bronze e gera um arquivo de metadados JSON.

[EN]
1. The `scrape_webpage(url)` function is called to extract data from the web page and load it into a Polars DataFrame.
2. The loaded DataFrame is passed to the `validate_dataframe(df)` function, which validates the data using a Pydantic model.
3. If validation succeeds, the validated DataFrame is sent to the `save_data_and_metadata(df, origin, framework)` function.
4. The `save_data_and_metadata` function saves the DataFrame as a CSV in the bronze directory and generates a JSON metadata file.

Dependências / Dependencies:
- polars
- requests
- beautifulsoup4
- pydantic
- python-dotenv
"""

import os
import json
import polars as pl
import requests
import pandas as pd  # Temporary bridge for HTML tables
from bs4 import BeautifulSoup
from datetime import datetime
from dotenv import load_dotenv

from utils.logger import setup_logger
from utils.pydantic_validation_template_polars import validate_with_pydantic_batch
from contracts.data_contracts_template import ProductWebScrapingContract

# Setup
logger = setup_logger("webscraping_ingestion_polars_template")
load_dotenv()

# Constantes
BRONZE_PATH = "./data/bronze/"

def generate_file_paths(origin: str, framework: str) -> tuple:
    """
    Gera os caminhos para salvar o arquivo de dados e o arquivo de metadados.
    Generate the paths to save the data file and the metadata file.

    Args:
        origin (str): origem dos dados / data source origin
        framework (str): framework utilizado / framework used

    Returns:
        tuple: output_data_file, output_metadata_file, file_name, timestamp
    """
    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    file_name = f"{origin}_{framework}_{timestamp}"

    output_data_file = os.path.join(BRONZE_PATH, f"{file_name}.csv")

    today = datetime.now()
    metadata_dir = os.path.join("metadata", str(today.year), f"{today.month:02d}", f"{today.day:02d}")
    os.makedirs(metadata_dir, exist_ok=True)
    output_metadata_file = os.path.join(metadata_dir, f"{file_name}_metadata.json")

    return output_data_file, output_metadata_file, file_name, timestamp

def scrape_webpage(url: str) -> pl.DataFrame:
    """
    Realiza scraping da página e retorna DataFrame Polars.
    Scrapes the webpage and returns a Polars DataFrame.

    Args:
        url (str): URL da página / URL of the page

    Returns:
        pl.DataFrame: DataFrame extraído / extracted DataFrame
    """
    try:
        response = requests.get(url)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")

        # Using pandas as bridge for HTML tables since Polars doesn't have direct HTML parsing
        tables = pd.read_html(str(soup))

        if not tables:
            logger.error("Nenhuma tabela encontrada na página / No table found on the page")
            return None

        # Convert pandas DataFrame to Polars
        df = pl.from_pandas(tables[0])  # Assuming first table
        logger.info(f"Tabela extraída com {df.height} linhas e {df.width} colunas / "
                   f"Table extracted with {df.height} rows and {df.width} columns")
        return df

    except Exception as e:
        logger.error(f"Erro no scraping: {str(e)} / Error during scraping: {str(e)}")
        return None

def validate_dataframe(df: pl.DataFrame) -> pl.DataFrame:
    """
    Valida o DataFrame usando contrato Pydantic.
    Validate the DataFrame using a Pydantic contract.

    Args:
        df (pl.DataFrame): DataFrame a ser validado / DataFrame to validate

    Returns:
        pl.DataFrame: DataFrame validado / validated DataFrame
    """
    try:
        if df is None:
            raise ValueError("DataFrame vazio para validação / Empty DataFrame for validation")

        validated_df = validate_with_pydantic_batch(df, ProductWebScrapingContract)
        return validated_df

    except Exception as e:
        logger.error(f"Erro na validação dos dados: {str(e)} / Error validating data: {str(e)}")
        return None

def save_data_and_metadata(df: pl.DataFrame, origin: str, framework: str) -> bool:
    """
    Salva o DataFrame validado e gera metadados.
    Save the validated DataFrame and generate metadata.

    Args:
        df (pl.DataFrame): DataFrame validado / validated DataFrame
        origin (str): origem dos dados / data source origin
        framework (str): framework utilizado / framework used

    Returns:
        bool: True se sucesso / True if successful
    """
    try:
        if df is None:
            logger.error("DataFrame vazio / Empty DataFrame")
            return False

        output_data_file, output_metadata_file, file_name, timestamp = generate_file_paths(origin, framework)

        # Save using Polars
        df.write_csv(output_data_file)
        logger.info(f"Dados salvos: {output_data_file} / Data saved: {output_data_file}")

        metadata = {
            "origin": origin,
            "framework": framework,
            "timestamp": timestamp,
            "status": "success",
            "data_file": output_data_file,
            "rows": df.height,
            "columns": df.width,
            "columns_types": {name: str(dtype) for name, dtype in zip(df.columns, df.dtypes)}
        }

        with open(output_metadata_file, "w", encoding="utf-8") as f:
            json.dump(metadata, f, ensure_ascii=False, indent=4)

        logger.info(f"Metadados salvos: {output_metadata_file} / Metadata saved: {output_metadata_file}")
        return True

    except Exception as e:
        logger.error(f"Erro ao salvar dados/metadados: {str(e)} / Error saving data/metadata: {str(e)}")
        return False

if __name__ == "__main__":
    try:
        url = os.getenv("WEB_SCRAPING_URL")
        origin = "webscraping"
        framework = "polars"

        df = scrape_webpage(url)
        if df is not None:
            os.makedirs(BRONZE_PATH, exist_ok=True)
            validated_df = validate_dataframe(df)
            if validated_df is not None:
                save_data_and_metadata(validated_df, origin, framework)

    except Exception as e:
        logger.error(f"Erro na execução principal: {str(e)} / Main execution error: {str(e)}")
