"""
SharePoint XLS Ingestion Template using Polars

Este é um template profissional para baixar arquivos XLS/XLSX do SharePoint,
armazenar os dados no diretório bronze e gerar automaticamente um arquivo de metadados.

ORIENTAÇÕES:
- Defina a URL do arquivo no SharePoint e o Token de Acesso no arquivo .env.
- Faça o download do arquivo Excel.
- Carregue a planilha como DataFrame Polars.
- Valide usando contratos Pydantic (Data Contracts).
- Salve o resultado como CSV.
- Gere também um arquivo de metadados (.json) organizado por data.

Obs: Para construir um bom sistema de ingestão de dados, consulte o arquivo INGESTION_MAIN_CONSIDERATIONS.md.

INSTRUCTIONS:
- Set the SharePoint file URL and Access Token in the .env file.
- Download the Excel file.
- Load the spreadsheet into a Polars DataFrame.
- Validate using Pydantic Data Contracts.
- Save as CSV in bronze directory.
- Generate metadata file (.json) organized by date.

Ps: To build a good data ingestion system, consult the INGESTION_MAIN_CONSIDERATIONS.md file.

Fluxo de Execução / Execution Flow:
-----------------------------------

[PT-BR]
1. A função `download_sharepoint_xls(url, token)` é chamada para baixar o arquivo XLS/XLSX do SharePoint.
2. O arquivo baixado é carregado usando a função `load_excel_file(file_path)`, que retorna um DataFrame Polars.
3. O DataFrame é passado para `validate_dataframe(df)`, que valida usando um modelo Pydantic.
4. Se a validação for bem-sucedida, o DataFrame validado é enviado para `save_data_and_metadata(df, origin, framework)`.

[EN]
1. The `download_sharepoint_xls(url, token)` function is called to download the XLS/XLSX file from SharePoint.
2. The downloaded file is loaded using `load_excel_file(file_path)`, returning a Polars DataFrame.
3. The DataFrame is passed to `validate_dataframe(df)`, which validates it using a Pydantic model.
4. If validation succeeds, the validated DataFrame is sent to `save_data_and_metadata(df, origin, framework)`.

Dependências / Dependencies:
- polars
- openpyxl
- requests
- pydantic
- python-dotenv
"""

import os
import json
import requests
import polars as pl
from datetime import datetime
from dotenv import load_dotenv

from utils.logger import setup_logger
from utils.pydantic_validation_template_polars import validate_with_pydantic_batch
from contracts.data_contracts import ProductSharePointContract

# Setup
logger = setup_logger("sharepoint_xls_ingestion_polars_template")
load_dotenv()

# Constantes
BRONZE_PATH = "./data/bronze/"
TEMP_PATH = "./data/temp/"

os.makedirs(BRONZE_PATH, exist_ok=True)
os.makedirs(TEMP_PATH, exist_ok=True)

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

def download_sharepoint_xls(url: str, token: str) -> str:
    """
    Baixa arquivo XLS/XLSX do SharePoint para pasta temporária.
    Download XLS/XLSX file from SharePoint to temp folder.

    Args:
        url (str): URL do arquivo no SharePoint
        token (str): Token de acesso / Access token

    Returns:
        str: Caminho do arquivo baixado / Path to the downloaded file
    """
    try:
        headers = {"Authorization": f"Bearer {token}"}
        response = requests.get(url, headers=headers)
        response.raise_for_status()

        filename = os.path.join(TEMP_PATH, "downloaded_file.xlsx")
        with open(filename, "wb") as f:
            f.write(response.content)

        logger.info(f"Arquivo baixado para: {filename} / File downloaded to: {filename}")
        return filename

    except Exception as e:
        logger.error(f"Erro ao baixar arquivo do SharePoint: {str(e)} / Error downloading file from SharePoint: {str(e)}")
        return None

def load_excel_file(file_path: str) -> pl.DataFrame:
    """
    Carrega arquivo Excel para DataFrame Polars.
    Load Excel file into a Polars DataFrame.

    Args:
        file_path (str): Caminho do arquivo / File path

    Returns:
        pl.DataFrame: DataFrame carregado / loaded DataFrame
    """
    try:
        # Polars doesn't have direct Excel support, so we use pandas as bridge
        import pandas as pd
        temp_df = pd.read_excel(file_path)
        df = pl.from_pandas(temp_df)
        
        logger.info(f"Arquivo Excel carregado com {df.height} linhas e {df.width} colunas / "
                   f"Excel file loaded with {df.height} rows and {df.width} columns")
        return df
    except Exception as e:
        logger.error(f"Erro ao carregar arquivo Excel: {str(e)} / Error loading Excel file: {str(e)}")
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

        validated_df = validate_with_pydantic_batch(df, ProductSharePointContract)
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
        url = os.getenv("SHAREPOINT_XLS_URL")
        token = os.getenv("SHAREPOINT_TOKEN")
        origin = "sharepoint_xls"
        framework = "polars"

        file_path = download_sharepoint_xls(url, token)
        if file_path:
            df = load_excel_file(file_path)
            if df is not None:
                validated_df = validate_dataframe(df)
                if validated_df is not None:
                    save_data_and_metadata(validated_df, origin, framework)

            # Clean up temporary file
            os.remove(file_path)
            logger.info(f"Arquivo temporário removido: {file_path} / Temporary file removed: {file_path}")

    except Exception as e:
        logger.error(f"Erro na execução principal: {str(e)} / Main execution error: {str(e)}")
