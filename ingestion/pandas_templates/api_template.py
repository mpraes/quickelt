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

Obs: Para construir um bom sistema de ingestão de dados, consulte o arquivo INGESTION_MAIN_CONSIDERATIONS.md.

INSTRUCTIONS:
- Set API URL and Token in .env.
- Make the request and load JSON response.
- Convert to Pandas DataFrame.
- Save as CSV in bronze directory.
- Generate metadata file (.json) organized by date.

Ps: To build a good data ingestion system, consult the INGESTION_MAIN_CONSIDERATIONS.md file.

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

def generate_file_paths(origin: str, format: str) -> tuple:
    """
    Gera os caminhos para salvar o arquivo de dados e o arquivo de metadados.
    Generate the paths to save the data file and the metadata file.
    args[en]:
        origin: origin of the data
        format: format of the data
    returns[en]:
        output_data_file: path to the data file
        output_metadata_file: path to the metadata file
        nome_arquivo: name of the file
        timestamp: timestamp of the file
    args[pt]:
        origin: origem dos dados
        format: formato dos dados
    returns[pt]:
        output_data_file: caminho para o arquivo de dados
        output_metadata_file: caminho para o arquivo de metadados
        nome_arquivo: nome do arquivo
        timestamp: timestamp do arquivo

    """
    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    file_name = f"{origin}_{format}_{timestamp}"

    # Diretórios
    output_data_file = os.path.join(BRONZE_PATH, f"{file_name}.csv")

    today = datetime.now()
    metadata_dir = os.path.join("metadata", str(today.year), f"{today.month:02d}", f"{today.day:02d}")
    os.makedirs(metadata_dir, exist_ok=True)
    output_metadata_file = os.path.join(metadata_dir, f"{file_name}_metadata.json")

    return output_data_file, output_metadata_file, file_name, timestamp

def ingest_api(origin: str, framework: str, url: str, token: str) -> pd.DataFrame:
    """
    Main function to ingest API data and return a DataFrame
    
    Args:
        origin (str): Origin of the data
        framework (str): Framework used (pandas/polars)
        url (str): API URL
        token (str): API token for authentication
        
    Returns:
        pd.DataFrame: DataFrame with the API data
        
    [PT-BR]
    Função principal para ingerir dados da API e retornar um DataFrame
    
    Args:
        origin (str): Origem dos dados
        framework (str): Framework utilizado (pandas/polars) 
        url (str): URL da API
        token (str): Token de autenticação da API
        
    Returns:
        pd.DataFrame: DataFrame com os dados da API
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
        df = None

        # Transform JSON to DataFrame / Transformar JSON em DataFrame
        if isinstance(data, list):
            df = pd.DataFrame(data)
        elif isinstance(data, dict):
            df = pd.DataFrame(data.get("results", []))
        else:
            logger.error("Formato de resposta da API não reconhecido / Unrecognized API response format")
            return None

        logger.info(f"DataFrame criado com {df.shape[0]} linhas e {df.shape[1]} colunas / DataFrame created with {df.shape[0]} rows and {df.shape[1]} columns")
        return df

    except requests.exceptions.RequestException as e:
        logger.error(f"Erro na requisição HTTP: {str(e)} / HTTP request error: {str(e)}")
        return None
    except json.JSONDecodeError as e:
        logger.error(f"Erro ao decodificar JSON: {str(e)} / JSON decode error: {str(e)}")
        return None
    except Exception as e:
        logger.error(f"Erro inesperado: {str(e)} / Unexpected error: {str(e)}")
        return None

def save_data_and_generate_metadata(bronze_path: str, df: pd.DataFrame, origin: str, framework: str) -> bool:
    """
    Save data as CSV and generate metadata
    
    Args:
        bronze_path (str): Path to save the data
        df (pd.DataFrame): DataFrame to save
        origin (str): Data origin
        framework (str): Framework used
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        if df is None:
            logger.error("DataFrame vazio / Empty DataFrame")
            return False

        output_data_file, output_metadata_file, file_name, timestamp = generate_file_paths(origin, framework)

        # Save data as CSV / Salva arquivo como CSV
        df.to_csv(output_data_file, index=False)
        logger.info(f"Dados salvos em: {output_data_file} / Data saved to: {output_data_file}")

        # Generate metadata / Gera Metadados
        metadata = {
            "origem": origin,
            "framework": framework,
            "timestamp": timestamp,
            "status": "success",
            "arquivo_dados": output_data_file,
            "linhas": df.shape[0],
            "colunas": df.shape[1],
            "colunas_tipos": df.dtypes.astype(str).to_dict()
        }

        with open(output_metadata_file, "w", encoding="utf-8") as f:
            json.dump(metadata, f, ensure_ascii=False, indent=4)
        
        logger.info(f"Metadados salvos em: {output_metadata_file} / Metadata saved to: {output_metadata_file}")
        return True

    except Exception as e:
        logger.error(f"Erro ao salvar dados/metadados: {str(e)} / Error saving data/metadata: {str(e)}")
        return False

if __name__ == "__main__":
    #Exemplo de uso das funções acima / Example of using the functions above
    try:
        url = os.getenv("API_URL")
        token = os.getenv("API_TOKEN")
        origin = "api"
        framework = "pandas"
        
        df = ingest_api(origin, framework, url, token)
        if df is not None:
            os.makedirs(BRONZE_PATH, exist_ok=True)
            save_data_and_generate_metadata(BRONZE_PATH, df, origin, framework)
        
    except Exception as e:
        logger.error(f"Erro na execução principal: {str(e)} / Main execution error: {str(e)}")

