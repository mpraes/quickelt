"""
Databases Ingestion Template PRO usando Polars

Este é um template profissional para conectar a bancos de dados relacionais (PostgreSQL, MySQL, OracleSQL, MSSQL)
usando Polars, com salvamento em Parquet, tentativas automáticas de conexão e execução,
validação de variáveis de ambiente e geração automática de metadados.

Professional template to connect to relational databases (PostgreSQL, MySQL, OracleSQL, MSSQL)
using Polars, with Parquet saving, automatic retries, environment variable validation,
and automatic metadata generation.

Dependências / Dependencies:
- polars
- pandas
- sqlalchemy
- psycopg2 / pymysql / oracledb / pyodbc
- python-dotenv
- tenacity

Fluxo de Ingestão de Dados / Data Ingestion Flow:

validate_env_variables()
    ↓
build_connection_string()
    ↓
connect_to_database()
    ↓
load_data_as_polars()
    ↓
generate_file_paths()
    ↓
save_polars_dataframe()
    ↓
generate_metadata()
"""

import os
import polars as pl
import pandas as pd
import json
from sqlalchemy import create_engine
from datetime import datetime
from dotenv import load_dotenv
from utils.logger import setup_logger
from tenacity import retry, stop_after_attempt, wait_fixed

# Setup
logger = setup_logger("databases_ingestion_polars_template_pro")
load_dotenv()

# Constantes / Constants
BRONZE_PATH = "./data/bronze/"
REQUIRED_ENV_VARS = ["DB_TYPE", "DB_HOST", "DB_PORT", "DB_USER", "DB_PASSWORD", "DB_DATABASE", "DB_QUERY"]

# ------------------- Funções Auxiliares / Helper Functions -------------------

def validate_env_variables():
    """
    Valida se todas as variáveis de ambiente obrigatórias estão presentes.
    Validate if all required environment variables are set.
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
def load_data_as_polars(query: str, engine) -> pl.DataFrame:
    """
    Executa a consulta SQL e carrega os dados como DataFrame Polars.
    Execute SQL query and load data as Polars DataFrame.
    """
    df_pandas = pd.read_sql_query(query, engine)
    return pl.DataFrame(df_pandas)

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

def save_polars_dataframe(df: pl.DataFrame, output_path: str):
    """
    Salva o DataFrame Polars em formato Parquet.
    Save Polars DataFrame in Parquet format.
    """
    df.write_parquet(f"{output_path}.parquet")

def generate_metadata(df: pl.DataFrame, query: str, output_file: str, output_metadata_file: str, origem: str, formato: str, timestamp: str):
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
        "output_file": f"{output_file}.parquet",
        "quantidade_linhas": df.shape[0],
        "quantidade_colunas": df.shape[1]
    }
    with open(output_metadata_file, "w", encoding="utf-8") as f:
        json.dump(metadata, f, ensure_ascii=False, indent=4)

# ------------------- Função Principal / Main Function -------------------

def ingest():
    """
    Função principal: executa o processo completo de ingestão.
    Main function: execute the full ingestion process.
    """
    origem = "database"
    formato = "polars"

    try:
        logger.info("Validando variáveis de ambiente / Validating environment variables")
        validate_env_variables()

        logger.info("Construindo connection string / Building connection string")
        connection_string = build_connection_string()

        logger.info(f"Conectando ao banco de dados / Connecting to database {os.getenv('DB_TYPE')}")
        engine = connect_to_database(connection_string)

        query = os.getenv("DB_QUERY")

        logger.info("Carregando dados / Loading data")
        df = load_data_as_polars(query=query, engine=engine)

        logger.info(f"Dados carregados: {df.shape[0]} linhas / Data loaded: {df.shape[0]} rows")

        os.makedirs(BRONZE_PATH, exist_ok=True)
        output_data_file, output_metadata_file, nome_arquivo, timestamp = generate_file_paths(origem, formato)

        logger.info("Salvando DataFrame como Parquet / Saving DataFrame as Parquet")
        save_polars_dataframe(df, output_data_file)

        logger.info("Gerando metadados / Generating metadata")
        generate_metadata(df, query, output_data_file, output_metadata_file, origem, formato, timestamp)

        logger.info("Processo de ingestão concluído com sucesso / Ingestion process successfully completed")

    except Exception as e:
        logger.error(f"Erro durante a ingestão: {str(e)} / Error during ingestion: {str(e)}")

# ------------------- Execução Direta / Direct Execution -------------------

if __name__ == "__main__":
    ingest()