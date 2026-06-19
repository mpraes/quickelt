"""
API Ingestion Template using Pandas

Este é um template profissional para consumir dados de uma API pública ou privada usando Pandas,
armazenar os dados no diretório bronze e gerar automaticamente um arquivo de metadados.

ORIENTAÇÕES:
- Defina a URL e Token da API no arquivo .env.
- Faça a requisição e carregue o JSON.
- Converta para DataFrame Pandas.
- Valide usando contratos Pydantic (Data Contracts).
- Salve o resultado como CSV.
- Gere também um arquivo de metadados (.json) organizado por data.

Obs: Para construir um bom sistema de ingestão de dados, consulte o arquivo INGESTION_MAIN_CONSIDERATIONS.md.

INSTRUCTIONS:
- Set API URL and Token in .env.
- Make the request and load JSON response.
- Convert to Pandas DataFrame.
- Validate using Pydantic Data Contracts.
- Save as CSV in bronze directory.
- Generate metadata file (.json) organized by date.

Ps: To build a good data ingestion system, consult the INGESTION_MAIN_CONSIDERATIONS.md file.

Dependências / Dependencies:
- pandas
- requests
- pydantic
- python-dotenv
"""

import os
import json
import pandas as pd
import requests
from datetime import datetime
from dotenv import load_dotenv

from utils.logger import setup_logger
from utils.pydantic_validation_template_pandas import validate_with_pydantic_batch
from contracts.data_contracts_template import ProductAPIContract 

# Setup
logger = setup_logger("api_ingestion_pandas_template")
load_dotenv()

# Constantes
BRONZE_PATH = "./data/bronze/"

def generate_file_paths(origin: str, framework: str) -> tuple:
    """
    Gera os caminhos para salvar o arquivo de dados e o arquivo de metadados.
    Generate the paths to save the data file and the metadata file.

    Args (PT-BR):
        origin (str): origem dos dados
        framework (str): framework utilizado

    Args (EN):
        origin (str): data source origin
        framework (str): framework used

    Returns (PT-BR):
        tuple: caminhos e timestamp gerados

    Returns (EN):
        tuple: generated paths and timestamp
    """
    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    file_name = f"{origin}_{framework}_{timestamp}"

    output_data_file = os.path.join(BRONZE_PATH, f"{file_name}.csv")

    today = datetime.now()
    metadata_dir = os.path.join("metadata", str(today.year), f"{today.month:02d}", f"{today.day:02d}")
    os.makedirs(metadata_dir, exist_ok=True)
    output_metadata_file = os.path.join(metadata_dir, f"{file_name}_metadata.json")

    return output_data_file, output_metadata_file, file_name, timestamp

def ingest_api(url: str, token: str) -> pd.DataFrame:
    """
    Faz requisição à API e retorna DataFrame.
    Send a request to the API and return a DataFrame.

    Args (PT-BR):
        url (str): URL da API
        token (str): token de autenticação

    Args (EN):
        url (str): API URL
        token (str): authentication token

    Returns:
        pd.DataFrame: dados extraídos da API
    """
    if not url:
        logger.error("API_URL não definida no .env / API_URL not defined in .env")
        return None

    headers = {"Authorization": f"Bearer {token}"} if token else {}

    try:
        logger.info(f"Enviando requisição para API: {url} / Sending request to API: {url}")
        response = requests.get(url, headers=headers)
        response.raise_for_status()

        data = response.json()

        if isinstance(data, list):
            df = pd.DataFrame(data)
        elif isinstance(data, dict):
            df = pd.DataFrame(data.get("results", []))
        else:
            logger.error("Formato de resposta da API não reconhecido / Unrecognized API response format")
            return None

        logger.info(f"DataFrame criado com {df.shape[0]} linhas e {df.shape[1]} colunas / DataFrame created with {df.shape[0]} rows and {df.shape[1]} columns")
        return df

    except Exception as e:
        logger.error(f"Erro ao ingerir API: {str(e)} / Error ingesting API: {str(e)}")
        return None

def validate_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Valida o DataFrame usando contrato Pydantic.
    Validate the DataFrame using a Pydantic contract.

    Args (PT-BR):
        df (pd.DataFrame): DataFrame a ser validado

    Args (EN):
        df (pd.DataFrame): DataFrame to validate

    Returns:
        pd.DataFrame: DataFrame validado
    """
    try:
        if df is None:
            raise ValueError("DataFrame vazio para validação / Empty DataFrame for validation")

        validated_df = validate_with_pydantic_batch(df, ProductAPIContract)
        return validated_df

    except Exception as e:
        logger.error(f"Erro na validação dos dados: {str(e)} / Error validating data: {str(e)}")
        return None

def save_data_and_metadata(df: pd.DataFrame, origin: str, framework: str) -> bool:
    """
    Salva o DataFrame validado e gera metadados.
    Save the validated DataFrame and generate metadata.

    Args (PT-BR):
        df (pd.DataFrame): DataFrame validado
        origin (str): origem dos dados
        framework (str): framework utilizado

    Args (EN):
        df (pd.DataFrame): validated DataFrame
        origin (str): data source origin
        framework (str): framework used

    Returns:
        bool: True se sucesso / True if successful
    """
    try:
        if df is None:
            logger.error("DataFrame vazio / Empty DataFrame")
            return False

        output_data_file, output_metadata_file, file_name, timestamp = generate_file_paths(origin, framework)

        df.to_csv(output_data_file, index=False)
        logger.info(f"Dados salvos: {output_data_file} / Data saved: {output_data_file}")

        metadata = {
            "origin": origin,
            "framework": framework,
            "timestamp": timestamp,
            "status": "success",
            "data_file": output_data_file,
            "rows": df.shape[0],
            "columns": df.shape[1],
            "columns_types": df.dtypes.astype(str).to_dict()
        }

        with open(output_metadata_file, "w", encoding="utf-8") as f:
            json.dump(metadata, f, ensure_ascii=False, indent=4)

        logger.info(f"Metadados salvos: {output_metadata_file} / Metadata saved: {output_metadata_file}")
        return True

    except Exception as e:
        logger.error(f"Erro ao salvar dados/metadados: {str(e)} / Error saving data/metadata: {str(e)}")
        return False

if __name__ == "__main__":
    # Exemplo de execução / Example of execution
    try:
        url = os.getenv("API_URL")
        token = os.getenv("API_TOKEN")
        origin = "api"
        framework = "pandas"

        df = ingest_api(url, token)
        if df is not None:
            os.makedirs(BRONZE_PATH, exist_ok=True)
            validated_df = validate_dataframe(df)
            if validated_df is not None:
                save_data_and_metadata(validated_df, origin, framework)

    except Exception as e:
        logger.error(f"Erro na execução principal: {str(e)} / Error in main execution: {str(e)}")
