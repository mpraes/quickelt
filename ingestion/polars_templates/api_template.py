"""
API Ingestion Template using Polars (JSON and XML)

Este é um template profissional para consumir dados de uma API pública ou privada usando Polars,
armazenar os dados no diretório bronze e gerar automaticamente um arquivo de metadados.

Agora suporta tanto APIs que retornam JSON quanto XML.

ORIENTAÇÕES:
- Defina a URL e Token da API no arquivo .env.
- Defina o formato da resposta esperada: "json" ou "xml".
- Faça a requisição e carregue a resposta corretamente.
- Converta para DataFrame Polars.
- Valide usando contratos Pydantic (Data Contracts).
- Salve o resultado como CSV.
- Gere também um arquivo de metadados (.json) organizado por data.

Obs: Para construir um bom sistema de ingestão de dados, consulte o arquivo INGESTION_MAIN_CONSIDERATIONS.md.

INSTRUCTIONS:
- Set the API URL and Token in the .env file.
- Define the expected response format: "json" or "xml".
- Make the request and correctly parse the response.
- Convert to a Polars DataFrame.
- Validate using Pydantic Data Contracts.
- Save as CSV in the bronze directory.
- Generate a metadata file (.json) organized by date.

Fluxo de Execução / Execution Flow:
-----------------------------------

[PT-BR]
1. A função `ingest_api(url, token, response_format)` é chamada para fazer a requisição e transformar a resposta em DataFrame.
2. O DataFrame carregado é passado para `validate_dataframe(df)`.
3. Se validado com sucesso, o DataFrame vai para `save_data_and_metadata(df, origin, framework)`.

[EN]
1. The `ingest_api(url, token, response_format)` function is called to make the request and transform the response into a DataFrame.
2. The loaded DataFrame is passed to `validate_dataframe(df)`.
3. If validated successfully, the DataFrame goes to `save_data_and_metadata(df, origin, framework)`.

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
from bs4 import BeautifulSoup
from datetime import datetime
from dotenv import load_dotenv

from utils.logger import setup_logger
from utils.pydantic_validation_template_polars import validate_with_pydantic_batch
from contracts.data_contracts_template import ProductAPIContract  # Ajuste para o seu contrato real

# Setup
logger = setup_logger("api_ingestion_polars_template")
load_dotenv()

# Constantes
BRONZE_PATH = "./data/bronze/"
os.makedirs(BRONZE_PATH, exist_ok=True)

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

def ingest_api(url: str, token: str, response_format: str = "json") -> pl.DataFrame:
    """
    Faz a requisição para a API e retorna o DataFrame Polars conforme o formato especificado.
    Makes the request to the API and returns the Polars DataFrame according to the specified format.

    Args:
        url (str): URL da API / API URL
        token (str): Token de autenticação / Authentication token
        response_format (str): "json" ou "xml" / "json" or "xml"

    Returns:
        pl.DataFrame: DataFrame carregado / loaded DataFrame
    """
    if not url:
        logger.error("API_URL não definida no .env / API_URL not defined in .env")
        return None

    headers = {"Authorization": f"Bearer {token}"} if token else {}

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()

        if response_format.lower() == "json":
            data = response.json()
            if isinstance(data, list):
                df = pl.DataFrame(data)
            elif isinstance(data, dict):
                df = pl.DataFrame(data.get("results", []))
            else:
                logger.error("Formato de resposta JSON não reconhecido / Unrecognized JSON response format")
                return None

        elif response_format.lower() == "xml":
            soup = BeautifulSoup(response.text, "xml")
            rows = []
            for item in soup.find_all("Record"):  # Adapte o nome do nó conforme seu XML
                row = {child.name: child.text for child in item.find_all()}
                rows.append(row)
            df = pl.DataFrame(rows)

        else:
            logger.error(f"Formato de resposta não suportado: {response_format} / Unsupported response format: {response_format}")
            return None

        logger.info(f"DataFrame criado com {df.height} linhas e {df.width} colunas / DataFrame created with {df.height} rows and {df.width} columns")
        return df

    except Exception as e:
        logger.error(f"Erro na ingestão da API: {str(e)} / API ingestion error: {str(e)}")
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

        validated_df = validate_with_pydantic_batch(df, ProductAPIContract)
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

        # Save as CSV using Polars
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
    # Exemplo de execução / Example of execution
    try:
        url = os.getenv("API_URL")
        token = os.getenv("API_TOKEN")
        response_format = os.getenv("API_RESPONSE_FORMAT", "json")  # Pode ser "json" ou "xml"
        origin = "api"
        framework = "polars"

        df = ingest_api(url, token, response_format)
        if df is not None:
            validated_df = validate_dataframe(df)
            if validated_df is not None:
                save_data_and_metadata(validated_df, origin, framework)

    except Exception as e:
        logger.error(f"Erro na execução principal: {str(e)} / Main execution error: {str(e)}")
