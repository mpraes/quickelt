"""
Sharepoint CSV Ingestion Template using Pandas

Este é um template profissional para carregar arquivos CSV de um servidor Sharepoint utilizando Pandas,
armazenar os dados no diretório bronze, e gerar automaticamente um arquivo de metadados.

This is a professional template for loading CSV files from a Sharepoint server using Pandas,
store the data in the bronze directory, and automatically generate a metadata file.

OBJETIVOS:
- Carregar arquivos de um endereço no sharepoint e trazer para camada bronze.
- Salvar os dados em formato CSV no diretório bronze.
- Gerar automaticamente um arquivo de metadados (.json) organizado por data.

OBJECTIVES:
- Load files from a source sharepoint address and bring them to the bronze layer.
- Save the data in CSV format in the bronze directory.
- Automatically generate a metadata file (.json) organized by date.

ORIENTAÇÕES:
- Precisa antes registrar o app no Azure Portal da empresa:
    - Entre no portal.azure.com
    - Crie um novo registro de aplicativo (Azure AD)
    - Configure permissões:
        - Files.ReadAll
        - Sites.Read.All
        - offline_access (para refresh token)
    - Precisa copiar essas informações para o .env
        - client_id
        - tenant_id
        - client_secret (se for confidencial)

Obs: Para construir um bom sistema de ingestão de dados, consulte o arquivo INGESTION_MAIN_CONSIDERATIONS.md.
        

INSTRUCTIONS:
- Needs to register the app in the Azure Portal of the company before:
    - Go to portal.azure.com
    - Create a new application registration (Azure AD)
    - Configure permissions:
        - Files.ReadAll
        - Sites.Read.All
        - offline_access (for refresh token)
    - Needs to copy these information to the .env
        - client_id
        - tenant_id
        - client_secret (if confidential)
        
Ps: To build a good data ingestion system, consult the INGESTION_MAIN_CONSIDERATIONS.md file.

Dependências / Dependencies:
- pandas
- python-dotenv
- msal
- requests
- io
"""

from msal import ConfidentialClientApplication
import requests
import os
import pandas as pd
import json
from datetime import datetime
from io import StringIO
from dotenv import load_dotenv

from utils.logger import setup_logger

# Setup
logger = setup_logger("csv_ingestion_pandas_template")

def generate_file_paths(origem: str, formato: str, BRONZE_PATH: str) -> tuple:
    """
    Gera os caminhos para salvar o arquivo de dados e o arquivo de metadados.
    """
    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    file_name = f"{origem}_{formato}_{timestamp}"

    output_data_file = os.path.join(BRONZE_PATH, f"{file_name}.csv")

    today = datetime.now()
    metadata_dir = os.path.join("metadata", str(today.year), f"{today.month:02d}", f"{today.day:02d}")
    os.makedirs(metadata_dir, exist_ok=True)
    output_metadata_file = os.path.join(metadata_dir, f"{file_name}_metadata.json")

    return output_data_file, output_metadata_file, file_name, timestamp

def get_access_token(client_id: str, authority: str, client_secret: str, scope: list) -> str:
    """
    Get SharePoint access token using MSAL authentication.
    
    Args:
        client_id (str): Azure AD application client ID
        authority (str): Azure AD authority URL
        client_secret (str): Azure AD application client secret
        scope (list): List of required scopes
        
    Returns:
        str: Access token if successful, None otherwise
        
    [PT-BR]
    Obtém token de acesso do SharePoint usando autenticação MSAL.
    
    Args:
        client_id (str): ID do cliente da aplicação Azure AD
        authority (str): URL de autoridade do Azure AD
        client_secret (str): Segredo do cliente da aplicação Azure AD
        scope (list): Lista de escopos necessários
        
    Returns:
        str: Token de acesso se bem sucedido, None caso contrário
    """
    try:
        logger.info("Iniciando autenticação MSAL / Starting MSAL authentication")
        
        app = ConfidentialClientApplication(
            client_id, 
            authority=authority, 
            client_credential=client_secret
        )
        
        token_response = app.acquire_token_for_client(scopes=scope)
        
        if 'access_token' in token_response:
            logger.info("Token de acesso obtido com sucesso / Access token successfully obtained")
            return token_response['access_token']
        else:
            error_desc = token_response.get('error_description', 'No error description')
            logger.error(f"Falha ao obter token: {error_desc} / Failed to get token: {error_desc}")
            return None
            
    except Exception as e:
        logger.error(f"Erro na autenticação MSAL: {str(e)} / MSAL authentication error: {str(e)}")
        return None

def ingest(access_token: str, site_url: str, file_path: str) -> None:
    """
    Main function to ingest CSV data from SharePoint and save data + metadata.
    
    Args:
        access_token (str): SharePoint access token
        site_url (str): SharePoint site URL
        file_path (str): Path to the file in SharePoint
        
    [PT-BR]
    Função principal para ingerir dados CSV do SharePoint e salvar dados + metadados.
    
    Args:
        access_token (str): Token de acesso do SharePoint
        site_url (str): URL do site SharePoint
        file_path (str): Caminho do arquivo no SharePoint
    """
    origem = "sharepoint_csv"
    formato = "pandas"

    try:
        # Setup headers for Graph API
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json"
        }

        # Get drive information
        drive_resp = requests.get(
            f"{site_url}/drive", 
            headers=headers
        )
        drive_resp.raise_for_status()
        drive_id = drive_resp.json().get('id')

        if not drive_id:
            logger.error("Drive ID não encontrado / Drive ID not found")
            return

        # Get file content
        logger.info(f"Buscando arquivo: {file_path} / Fetching file: {file_path}")
        file_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{file_path}:/content"
        file_resp = requests.get(file_url, headers=headers)
        file_resp.raise_for_status()

        # Convert response content to DataFrame
        content = StringIO(file_resp.content.decode('utf-8'))
        df = pd.read_csv(content)

        logger.info(f"DataFrame carregado: {df.shape[0]} linhas, {df.shape[1]} colunas / "
                   f"DataFrame loaded: {df.shape[0]} rows, {df.shape[1]} columns")

        # Create bronze directory if it doesn't exist
        os.makedirs(BRONZE_PATH, exist_ok=True)

        # Generate file paths
        output_data_file, output_metadata_file, file_name, timestamp = generate_file_paths(origem, formato)

        # Save data
        df.to_csv(output_data_file, index=False)
        logger.info(f"Dados salvos em: {output_data_file} / Data saved to: {output_data_file}")

        # Generate metadata
        metadata = {
            "origem": origem,
            "formato": formato,
            "timestamp": timestamp,
            "status": "success",
            "site_url": site_url,
            "sharepoint_path": file_path,
            "output_file": output_data_file,
            "linhas": df.shape[0],
            "colunas": df.shape[1],
            "colunas_tipos": df.dtypes.astype(str).to_dict()
        }

        # Save metadata
        with open(output_metadata_file, "w", encoding="utf-8") as f:
            json.dump(metadata, f, ensure_ascii=False, indent=4)

        logger.info(f"Metadados salvos em: {output_metadata_file} / "
                   f"Metadata saved to: {output_metadata_file}")

    except requests.exceptions.RequestException as e:
        logger.error(f"Erro na requisição SharePoint: {str(e)} / SharePoint request error: {str(e)}")
    except pd.errors.EmptyDataError:
        logger.error("Arquivo CSV vazio / Empty CSV file")
    except Exception as e:
        logger.error(f"Erro inesperado: {str(e)} / Unexpected error: {str(e)}")

# Update main execution
if __name__ == "__main__":
    load_dotenv()
    BRONZE_PATH = "./data/bronze/"
    
    # Get configuration from environment variables
    client_id = os.getenv("SHAREPOINT_CLIENT_ID")
    tenant_id = os.getenv("SHAREPOINT_TENANT_ID")
    client_secret = os.getenv("SHAREPOINT_CLIENT_SECRET")
    site_url = os.getenv("SHAREPOINT_SITE_URL")
    file_path = os.getenv("SHAREPOINT_FILE_PATH")
    
    if not all([client_id, tenant_id, client_secret, site_url, file_path]):
        logger.error("Configurações do SharePoint ausentes no .env / Missing SharePoint settings in .env")
        exit(1)
    
    try:
        # Get access token
        authority = f"https://login.microsoftonline.com/{tenant_id}"
        scope = ["https://graph.microsoft.com/.default"]
        
        access_token = get_access_token(
            client_id=client_id,
            authority=authority,
            client_secret=client_secret,
            scope=scope
        )
        
        # Execute ingestion
        ingest(access_token, site_url, file_path)
        
    except Exception as e:
        logger.error(f"Erro na execução principal: {str(e)} / Main execution error: {str(e)}")
