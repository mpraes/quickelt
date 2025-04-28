"""
Databases Ingestion Template using Polars

Este é um template profissional para conectar a bancos de dados relacionais (PostgreSQL, MySQL, OracleSQL, MSSQL)
utilizando Polars, armazenar os dados no diretório bronze e gerar automaticamente um arquivo de metadados.

ORIENTAÇÕES:
- Configure as variáveis de ambiente no .env (host, user, password, database, driver, query).
- Conecte ao banco utilizando SQLAlchemy.
- Carregue os dados.
- Salve como Parquet.
- Gere um arquivo de metadados (.json) organizado por data.

INSTRUCTIONS:
- Configure environment variables in .env (host, user, password, database, driver, query).
- Connect to the database using SQLAlchemy.
- Load data.
- Save as Parquet into bronze directory.
- Generate metadata file (.json) organized by date.

Dependências / Dependencies:
- polars
- pandas
- sqlalchemy
- psycopg2 / pymysql / cx_Oracle / pyodbc
- python-dotenv
"""

import os
import polars as pl
import pandas as pd
import json
from sqlalchemy import create_engine
from datetime import datetime
from dotenv import load_dotenv

from utils.logger import setup_logger

# Setup
logger = setup_logger("databases_ingestion_polars_template")
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

def build_connection_string():
    """
    Constrói a connection string baseada nas variáveis de ambiente.
    Builds the connection string based on environment variables.
    """
    db_type = os.getenv("DB_TYPE")     # postgresql, mysql, oracle, mssql
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
        return f"oracle+cx_oracle://{user}:{password}@{host}:{port}/?service_name={database}"
    elif db_type == "mssql":
        return f"mssql+pyodbc://{user}:{password}@{host}:{port}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
    else:
        raise ValueError("Tipo de banco de dados inválido / Invalid database type")

def ingest():
    """
    Função principal para conectar ao banco de dados, executar a consulta e salvar dados + metadados.
    Main function to connect to the database, run the query and save data + metadata.
    """

    origem = "database"
    formato = "polars"

    try:
        logger.info("Construindo a connection string / Building the connection string")
        connection_string = build_connection_string()

        logger.info(f"Conectando ao banco de dados {os.getenv('DB_TYPE')} / Connecting to database {os.getenv('DB_TYPE')}")
        engine = create_engine(connection_string)

        query = os.getenv("DB_QUERY")
        if not query:
            logger.error("DB_QUERY não definida no .env / DB_QUERY not defined in .env")
            return

        # Lê usando Pandas e converte para Polars
        df_pandas = pd.read_sql_query(query, engine)
        df_polars = pl.DataFrame(df_pandas)

        logger.info(f"Consulta executada com sucesso, {df_polars.shape[0]} linhas retornadas / Query executed successfully, {df_polars.shape[0]} rows retrieved")

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
            "query": query,
            "output_file": output_data_file,
            "quantidade_linhas": df_polars.shape[0],
            "quantidade_colunas": df_polars.shape[1]
        }

        with open(output_metadata_file, "w", encoding="utf-8") as f:
            json.dump(metadata, f, ensure_ascii=False, indent=4)

        logger.info(f"Metadados salvos em {output_metadata_file} / Metadata saved in {output_metadata_file}")

    except Exception as e:
        logger.error(f"Erro durante a ingestão do banco de dados: {str(e)} / Error during database ingestion: {str(e)}")

if __name__ == "__main__":
    ingest()
