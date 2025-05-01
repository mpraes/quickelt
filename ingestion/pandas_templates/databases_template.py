"""
Databases Ingestion Template PRO usando Pandas

Este é um template profissional para conectar a bancos de dados relacionais (PostgreSQL, MySQL, OracleSQL, MSSQL)
utilizando Pandas, com gravação flexível em CSV ou Parquet, tentativas automáticas de conexão e execução,
validação de variáveis de ambiente e geração automática de metadados.

ORIENTAÇÕES:
- Configure as variáveis de ambiente no arquivo .env (DB_TYPE, DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_DATABASE, DB_QUERY, OUTPUT_FORMAT).
- Conecte ao banco de dados utilizando SQLAlchemy.
- Execute a consulta SQL.
- Carregue os dados usando Pandas.
- Salve o resultado no formato especificado (CSV ou Parquet) no diretório bronze.
- Gere automaticamente um arquivo de metadados (.json) organizado por data.

Obs: Para mais boas práticas e orientações, consulte o arquivo INGESTION_MAIN_CONSIDERATIONS.md.

Dependências:
- pandas
- sqlalchemy
- psycopg2 / pymysql / oracledb / pyodbc
- python-dotenv
- tenacity


Databases Ingestion Template PRO using Pandas

This is a professional template to connect to relational databases (PostgreSQL, MySQL, OracleSQL, MSSQL)
using Pandas, with flexible saving in CSV or Parquet, automatic retry on connection and query execution,
environment variable validation, and automatic metadata generation.

INSTRUCTIONS:
- Configure environment variables in the .env file (DB_TYPE, DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_DATABASE, DB_QUERY, OUTPUT_FORMAT).
- Connect to the database using SQLAlchemy.
- Execute the SQL query.
- Load data with Pandas.
- Save the result in the specified format (CSV or Parquet) into the bronze directory.
- Automatically generate a metadata file (.json) organized by date.

Note: For more best practices and guidance, consult the INGESTION_MAIN_CONSIDERATIONS.md file.

Dependencies:
- pandas
- sqlalchemy
- psycopg2 / pymysql / oracledb / pyodbc
- python-dotenv
- tenacity

Fluxo de Ingestão de Dados / Data Ingestion Flow:

[validate_env_variables()]
        ↓
[build_connection_string()]
        ↓
[connect_to_database()]
        ↓
[load_data()]
        ↓
[generate_file_paths()]
        ↓
[save_dataframe()]
        ↓
[generate_metadata()]
"""

import os
import pandas as pd
import json
from sqlalchemy import create_engine
from datetime import datetime
from dotenv import load_dotenv
from utils.logger import setup_logger
from tenacity import retry, stop_after_attempt, wait_fixed

# Setup
logger = setup_logger("databases_ingestion_modular_template")
load_dotenv()

# Constantes / Constants
BRONZE_PATH = "./data/bronze/"
REQUIRED_ENV_VARS = [
    "DB_TYPE", "DB_HOST", "DB_PORT", "DB_USER", "DB_PASSWORD",
    "DB_DATABASE", "DB_QUERY", "OUTPUT_FORMAT"
]

# ------------------- Funções Auxiliares / Helper Functions -------------------

def validate_env_variables():
    """
    Valida as variáveis de ambiente obrigatórias.
    Validate required environment variables.
    """
    missing = [var for var in REQUIRED_ENV_VARS if not os.getenv(var)]
    if missing:
        raise EnvironmentError(f"Variáveis faltando / Missing environment variables: {', '.join(missing)}")

def build_connection_string() -> str:
    """
    Constrói a connection string baseada nas variáveis de ambiente.
    Build the connection string based on environment variables.
    """
    db_type = os.getenv("DB_TYPE").lower()
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    database = os.getenv("DB_DATABASE")

    if db_type == "postgresql":
        return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
    elif db_type == "mysql":
        return f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
    elif db_type == "oracle":
        return f"oracle+oracledb://{user}:{password}@{host}:{port}/{database}"
    elif db_type == "mssql":
        return f"mssql+pyodbc://{user}:{password}@{host}:{port}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
    else:
        raise ValueError("Tipo de banco inválido / Invalid database type")

def connect_to_database(connection_string: str):
    """
    Cria o objeto engine de conexão com o banco de dados.
    Create SQLAlchemy engine for database connection.
    """
    return create_engine(connection_string)

@retry(stop=stop_after_attempt(3), wait=wait_fixed(5))
def load_data(query: str, engine) -> pd.DataFrame:
    """
    Executa a consulta SQL e carrega o DataFrame com tentativas automáticas.
    Execute SQL query and load DataFrame with automatic retries.
    """
    return pd.read_sql_query(query, engine)

def generate_file_paths(origem: str, formato: str) -> tuple:
    """
    Gera os caminhos dos arquivos de dados e metadados.
    Generate paths for data and metadata files.
    """
    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    nome_arquivo = f"{origem}_{formato}_{timestamp}"

    output_data_file = os.path.join(BRONZE_PATH, nome_arquivo)

    today = datetime.now()
    metadata_dir = os.path.join("metadata", str(today.year), f"{today.month:02d}", f"{today.day:02d}")
    os.makedirs(metadata_dir, exist_ok=True)
    output_metadata_file = os.path.join(metadata_dir, f"{nome_arquivo}_metadata.json")

    return output_data_file, output_metadata_file, nome_arquivo, timestamp

def save_dataframe(df: pd.DataFrame, output_path: str, format_: str):
    """
    Salva o DataFrame no formato especificado (CSV ou Parquet).
    Save DataFrame in the specified format (CSV or Parquet).
    """
    if format_ == "csv":
        df.to_csv(f"{output_path}.csv", index=False)
    elif format_ == "parquet":
        df.to_parquet(f"{output_path}.parquet", index=False)
    else:
        raise ValueError("Formato de saída inválido / Invalid output format")

def generate_metadata(df: pd.DataFrame, query: str, output_file: str, output_metadata_file: str, output_format: str, origem: str, formato: str, timestamp: str):
    """
    Gera e salva o arquivo de metadados (.json).
    Generate and save metadata file (.json).
    """
    metadata = {
        "origem": origem,
        "formato": formato,
        "timestamp": timestamp,
        "status": "success",
        "query": query,
        "output_file": f"{output_file}.{output_format}",
        "quantidade_linhas": df.shape[0],
        "quantidade_colunas": df.shape[1]
    }
    with open(output_metadata_file, "w", encoding="utf-8") as f:
        json.dump(metadata, f, ensure_ascii=False, indent=4)

# ------------------- Função Principal / Main Function -------------------

def ingest():
    """
    Função principal: executa todo o processo de ingestão de dados.
    Main function: executes the full data ingestion process.
    """
    origem = "database"
    formato = "pandas"

    try:
        logger.info("Validando variáveis de ambiente / Validating environment variables")
        validate_env_variables()

        logger.info("Construindo connection string / Building connection string")
        connection_string = build_connection_string()

        logger.info(f"Conectando ao banco de dados / Connecting to database {os.getenv('DB_TYPE')}")
        engine = connect_to_database(connection_string)

        query = os.getenv("DB_QUERY")
        output_format = os.getenv("OUTPUT_FORMAT").lower()

        logger.info("Carregando dados / Loading data")
        df = load_data(query=query, engine=engine)

        logger.info(f"Dados carregados: {df.shape[0]} linhas / Data loaded: {df.shape[0]} rows")

        os.makedirs(BRONZE_PATH, exist_ok=True)
        output_data_file, output_metadata_file, nome_arquivo, timestamp = generate_file_paths(origem, formato)

        logger.info(f"Salvando arquivo no formato {output_format} / Saving file in {output_format} format")
        save_dataframe(df, output_data_file, output_format)

        logger.info("Gerando metadados / Generating metadata")
        generate_metadata(df, query, output_data_file, output_metadata_file, output_format, origem, formato, timestamp)

        logger.info("Processo de ingestão concluído com sucesso / Ingestion process successfully completed")

    except Exception as e:
        logger.error(f"Erro durante a ingestão: {str(e)} / Error during ingestion: {str(e)}")

# ------------------- Execução Direta / Direct Execution -------------------

if __name__ == "__main__":
    ingest()
