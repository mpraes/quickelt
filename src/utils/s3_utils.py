"""
AWS S3 Utilities for QuickELT Project
-------------------------------------

This module provides common operations for AWS S3 using Python.
Includes functions for client creation, file operations, and data reading.

[PT-BR]
Utilitários AWS S3 para o Projeto QuickELT
------------------------------------------

Este módulo fornece operações comuns para AWS S3 usando Python.
Inclui funções para criação de cliente, operações de arquivo e leitura de dados.
"""

import os
import json
import logging
from typing import Optional, List, Dict, Any, Union
from pathlib import Path

import boto3
import pandas as pd
import polars as pl
import pyarrow.parquet as pq
import pyarrow as pa
from botocore.exceptions import ClientError, NoCredentialsError

# Import project logger
from .logger import get_logger

logger = get_logger(__name__)

# Load environment variables from .env file
# Carrega variáveis de ambiente do arquivo .env
from dotenv import load_dotenv
load_dotenv()

# Default configuration from environment variables
# Configuração padrão das variáveis de ambiente
DEFAULT_BUCKET = os.getenv('AWS_S3_BUCKET', 'quickelt-bucket')
AWS_REGION = os.getenv('AWS_REGION', 'us-east-1')
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_SESSION_TOKEN = os.getenv('AWS_SESSION_TOKEN')  # Optional for temporary credentials
AWS_S3_ENDPOINT_URL = os.getenv('AWS_S3_ENDPOINT_URL')  # Optional for S3-compatible services

# S3 Paths from environment variables
# Caminhos S3 das variáveis de ambiente
AWS_S3_BRONZE_PATH = os.getenv('AWS_S3_BRONZE_PATH', 'data/bronze/')
AWS_S3_SILVER_PATH = os.getenv('AWS_S3_SILVER_PATH', 'data/silver/')
AWS_S3_GOLD_PATH = os.getenv('AWS_S3_GOLD_PATH', 'data/gold/')
AWS_S3_TEMP_PATH = os.getenv('AWS_S3_TEMP_PATH', 'data/temp/')

# Supported file formats
SUPPORTED_FORMATS = {
    'csv': ['.csv'],
    'json': ['.json', '.jsonl'],
    'parquet': ['.parquet', '.pq'],
    'orc': ['.orc'],
    'excel': ['.xlsx', '.xls'],
    'avro': ['.avro']
}


def get_s3_client(
    region_name: Optional[str] = None,
    aws_access_key_id: Optional[str] = None,
    aws_secret_access_key: Optional[str] = None,
    aws_session_token: Optional[str] = None,
    endpoint_url: Optional[str] = None
) -> boto3.client:
    """
    Creates and returns an S3 client with specified credentials.
    
    [PT-BR]
    Cria e retorna um cliente S3 com as credenciais especificadas.
    
    Args:
        region_name (str, optional): AWS region name (defaults to AWS_REGION env var)
                                    Nome da região AWS (padrão: variável AWS_REGION)
        aws_access_key_id (str, optional): AWS access key ID (defaults to env var)
                                          ID da chave de acesso AWS (padrão: variável de ambiente)
        aws_secret_access_key (str, optional): AWS secret access key (defaults to env var)
                                              Chave secreta de acesso AWS (padrão: variável de ambiente)
        aws_session_token (str, optional): AWS session token for temporary credentials
                                          Token de sessão AWS para credenciais temporárias
        endpoint_url (str, optional): Custom endpoint URL for S3-compatible services
                                     URL de endpoint customizada para serviços compatíveis com S3
    
    Returns:
        boto3.client: Configured S3 client
                     Cliente S3 configurado
    
    Raises:
        NoCredentialsError: If credentials are not found
                           Se as credenciais não forem encontradas
    """
    try:
        # Use environment variables as defaults if not provided
        # Usa variáveis de ambiente como padrão se não fornecidas
        region_name = region_name or AWS_REGION
        aws_access_key_id = aws_access_key_id or AWS_ACCESS_KEY_ID
        aws_secret_access_key = aws_secret_access_key or AWS_SECRET_ACCESS_KEY
        aws_session_token = aws_session_token or AWS_SESSION_TOKEN
        endpoint_url = endpoint_url or AWS_S3_ENDPOINT_URL
        
        # Build client configuration
        # Constrói configuração do cliente
        client_config = {
            'region_name': region_name
        }
        
        # Add endpoint URL if provided (for S3-compatible services like MinIO)
        # Adiciona URL do endpoint se fornecida (para serviços compatíveis com S3 como MinIO)
        if endpoint_url:
            client_config['endpoint_url'] = endpoint_url
        
        # Add credentials if provided
        # Adiciona credenciais se fornecidas
        if aws_access_key_id and aws_secret_access_key:
            client_config.update({
                'aws_access_key_id': aws_access_key_id,
                'aws_secret_access_key': aws_secret_access_key
            })
            
            # Add session token if provided (for temporary credentials)
            # Adiciona token de sessão se fornecido (para credenciais temporárias)
            if aws_session_token:
                client_config['aws_session_token'] = aws_session_token
        
        s3_client = boto3.client('s3', **client_config)
        
        logger.info(f"S3 client created successfully for region: {region_name}")
        if endpoint_url:
            logger.info(f"Using custom endpoint: {endpoint_url}")
        return s3_client
    
    except NoCredentialsError as e:
        logger.error("AWS credentials not found. Please configure your credentials in .env file.")
        logger.error("Credenciais AWS não encontradas. Configure suas credenciais no arquivo .env.")
        raise
    except Exception as e:
        logger.error(f"Error creating S3 client: {str(e)}")
        logger.error(f"Erro ao criar cliente S3: {str(e)}")
        raise


def get_s3_paths() -> Dict[str, str]:
    """
    Get S3 paths from environment variables.
    
    [PT-BR]
    Obtém caminhos S3 das variáveis de ambiente.
    
    Returns:
        Dict[str, str]: Dictionary with S3 paths
                       Dicionário com caminhos S3
    """
    return {
        'bronze': AWS_S3_BRONZE_PATH,
        'silver': AWS_S3_SILVER_PATH,
        'gold': AWS_S3_GOLD_PATH,
        'temp': AWS_S3_TEMP_PATH
    }


def check_file_exists(
    bucket: Optional[str] = None,
    key: str = '',
    s3_client: Optional[boto3.client] = None
) -> bool:
    """
    Checks if a file exists in S3 bucket.
    
    [PT-BR]
    Verifica se um arquivo existe no bucket S3.
    
    Args:
        bucket (str, optional): S3 bucket name (defaults to AWS_S3_BUCKET env var)
                               Nome do bucket S3 (padrão: variável AWS_S3_BUCKET)
        key (str): S3 object key
                  Chave do objeto S3
        s3_client (boto3.client, optional): S3 client instance
                                           Instância do cliente S3
    
    Returns:
        bool: True if file exists, False otherwise
              True se o arquivo existir, False caso contrário
    """
    try:
        bucket = bucket or DEFAULT_BUCKET
        
        if s3_client is None:
            s3_client = get_s3_client()
        
        s3_client.head_object(Bucket=bucket, Key=key)
        logger.debug(f"File exists: s3://{bucket}/{key}")
        return True
    
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            logger.debug(f"File does not exist: s3://{bucket}/{key}")
            return False
        else:
            logger.error(f"Error checking file existence: {str(e)}")
            logger.error(f"Erro ao verificar existência do arquivo: {str(e)}")
            raise
    except Exception as e:
        logger.error(f"Unexpected error checking file existence: {str(e)}")
        logger.error(f"Erro inesperado ao verificar existência do arquivo: {str(e)}")
        raise


def list_objects(
    prefix: str = '',
    suffix: str = '',
    bucket: Optional[str] = None,
    s3_client: Optional[boto3.client] = None
) -> List[str]:
    """
    Lists objects in S3 bucket with optional prefix and suffix filtering.
    
    [PT-BR]
    Lista objetos no bucket S3 com filtros opcionais de prefixo e sufixo.
    
    Args:
        prefix (str): Object key prefix filter
                     Filtro de prefixo da chave do objeto
        suffix (str): Object key suffix filter
                     Filtro de sufixo da chave do objeto
        bucket (str, optional): S3 bucket name (defaults to AWS_S3_BUCKET env var)
                               Nome do bucket S3 (padrão: variável AWS_S3_BUCKET)
        s3_client (boto3.client, optional): S3 client instance
                                           Instância do cliente S3
    
    Returns:
        List[str]: List of object keys matching the criteria
                  Lista de chaves de objetos que atendem aos critérios
    
    Raises:
        Exception: If listing objects fails
                  Se a listagem de objetos falhar
    """
    try:
        bucket = bucket or DEFAULT_BUCKET
        
        if s3_client is None:
            s3_client = get_s3_client()
        
        paginator = s3_client.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)
        
        object_keys = []
        for page in page_iterator:
            if 'Contents' in page:
                for obj in page['Contents']:
                    key = obj['Key']
                    if not suffix or key.endswith(suffix):
                        object_keys.append(key)
        
        logger.info(f"Found {len(object_keys)} objects in s3://{bucket}/{prefix}")
        logger.info(f"Encontrados {len(object_keys)} objetos em s3://{bucket}/{prefix}")
        return object_keys
    
    except Exception as e:
        logger.error(f"Error listing objects in {bucket}/{prefix}: {str(e)}")
        logger.error(f"Erro ao listar objetos em {bucket}/{prefix}: {str(e)}")
        raise


def read_csv_from_s3(
    bucket: str,
    key: str,
    engine: str = 'pandas',
    **kwargs
) -> Union[pd.DataFrame, pl.DataFrame]:
    """
    Reads CSV file from S3 using specified engine.
    
    [PT-BR]
    Lê arquivo CSV do S3 usando o motor especificado.
    
    Args:
        bucket (str): S3 bucket name
                     Nome do bucket S3
        key (str): S3 object key
                  Chave do objeto S3
        engine (str): Engine to use ('pandas' or 'polars')
                     Motor a usar ('pandas' ou 'polars')
        **kwargs: Additional arguments for pandas.read_csv or polars.read_csv
                 Argumentos adicionais para pandas.read_csv ou polars.read_csv
    
    Returns:
        Union[pd.DataFrame, pl.DataFrame]: DataFrame with CSV data
                                         DataFrame com dados do CSV
    """
    try:
        s3_client = get_s3_client()
        
        # Get object from S3
        response = s3_client.get_object(Bucket=bucket, Key=key)
        
        if engine.lower() == 'pandas':
            df = pd.read_csv(response['Body'], **kwargs)
        elif engine.lower() == 'polars':
            df = pl.read_csv(response['Body'], **kwargs)
        else:
            raise ValueError(f"Unsupported engine: {engine}. Use 'pandas' or 'polars'")
        
        logger.info(f"Successfully read CSV from s3://{bucket}/{key} using {engine}")
        logger.info(f"CSV lido com sucesso de s3://{bucket}/{key} usando {engine}")
        return df
    
    except Exception as e:
        logger.error(f"Error reading CSV from s3://{bucket}/{key}: {str(e)}")
        logger.error(f"Erro ao ler CSV de s3://{bucket}/{key}: {str(e)}")
        raise


def read_json_from_s3(
    bucket: str,
    key: str,
    engine: str = 'pandas',
    **kwargs
) -> Union[pd.DataFrame, pl.DataFrame, Dict[str, Any]]:
    """
    Reads JSON file from S3 using specified engine.
    
    [PT-BR]
    Lê arquivo JSON do S3 usando o motor especificado.
    
    Args:
        bucket (str): S3 bucket name
                     Nome do bucket S3
        key (str): S3 object key
                  Chave do objeto S3
        engine (str): Engine to use ('pandas', 'polars', or 'json')
                     Motor a usar ('pandas', 'polars' ou 'json')
        **kwargs: Additional arguments for the reading function
                 Argumentos adicionais para a função de leitura
    
    Returns:
        Union[pd.DataFrame, pl.DataFrame, Dict[str, Any]]: Data from JSON file
                                                          Dados do arquivo JSON
    """
    try:
        s3_client = get_s3_client()
        response = s3_client.get_object(Bucket=bucket, Key=key)
        
        if engine.lower() == 'pandas':
            if key.endswith('.jsonl'):
                df = pd.read_json(response['Body'], lines=True, **kwargs)
            else:
                df = pd.read_json(response['Body'], **kwargs)
        elif engine.lower() == 'polars':
            if key.endswith('.jsonl'):
                df = pl.read_ndjson(response['Body'], **kwargs)
            else:
                df = pl.read_json(response['Body'], **kwargs)
        elif engine.lower() == 'json':
            content = response['Body'].read().decode('utf-8')
            data = json.loads(content)
            return data
        else:
            raise ValueError(f"Unsupported engine: {engine}. Use 'pandas', 'polars', or 'json'")
        
        logger.info(f"Successfully read JSON from s3://{bucket}/{key} using {engine}")
        logger.info(f"JSON lido com sucesso de s3://{bucket}/{key} usando {engine}")
        return df
    
    except Exception as e:
        logger.error(f"Error reading JSON from s3://{bucket}/{key}: {str(e)}")
        logger.error(f"Erro ao ler JSON de s3://{bucket}/{key}: {str(e)}")
        raise


def read_parquet_from_s3(
    bucket: str,
    key: str,
    engine: str = 'pandas',
    **kwargs
) -> Union[pd.DataFrame, pl.DataFrame]:
    """
    Reads Parquet file from S3 using specified engine.
    
    [PT-BR]
    Lê arquivo Parquet do S3 usando o motor especificado.
    
    Args:
        bucket (str): S3 bucket name
                     Nome do bucket S3
        key (str): S3 object key
                  Chave do objeto S3
        engine (str): Engine to use ('pandas' or 'polars')
                     Motor a usar ('pandas' ou 'polars')
        **kwargs: Additional arguments for the reading function
                 Argumentos adicionais para a função de leitura
    
    Returns:
        Union[pd.DataFrame, pl.DataFrame]: DataFrame with Parquet data
                                         DataFrame com dados do Parquet
    """
    try:
        s3_client = get_s3_client()
        response = s3_client.get_object(Bucket=bucket, Key=key)
        
        if engine.lower() == 'pandas':
            df = pd.read_parquet(response['Body'], **kwargs)
        elif engine.lower() == 'polars':
            df = pl.read_parquet(response['Body'], **kwargs)
        else:
            raise ValueError(f"Unsupported engine: {engine}. Use 'pandas' or 'polars'")
        
        logger.info(f"Successfully read Parquet from s3://{bucket}/{key} using {engine}")
        logger.info(f"Parquet lido com sucesso de s3://{bucket}/{key} usando {engine}")
        return df
    
    except Exception as e:
        logger.error(f"Error reading Parquet from s3://{bucket}/{key}: {str(e)}")
        logger.error(f"Erro ao ler Parquet de s3://{bucket}/{key}: {str(e)}")
        raise


def read_file_from_s3(
    bucket: str,
    key: str,
    file_format: Optional[str] = None,
    engine: str = 'pandas',
    **kwargs
) -> Union[pd.DataFrame, pl.DataFrame, Dict[str, Any]]:
    """
    Generic function to read files from S3 based on file extension or format.
    
    [PT-BR]
    Função genérica para ler arquivos do S3 baseado na extensão ou formato do arquivo.
    
    Args:
        bucket (str): S3 bucket name
                     Nome do bucket S3
        key (str): S3 object key
                  Chave do objeto S3
        file_format (str, optional): File format override
                                   Sobrescrever formato do arquivo
        engine (str): Engine to use for reading
                     Motor a usar para leitura
        **kwargs: Additional arguments for the reading function
                 Argumentos adicionais para a função de leitura
    
    Returns:
        Union[pd.DataFrame, pl.DataFrame, Dict[str, Any]]: Data from file
                                                          Dados do arquivo
    """
    try:
        # Determine file format
        if file_format is None:
            file_extension = Path(key).suffix.lower()
            for format_name, extensions in SUPPORTED_FORMATS.items():
                if file_extension in extensions:
                    file_format = format_name
                    break
        
        if file_format is None:
            raise ValueError(f"Unsupported file format for key: {key}")
        
        # Read based on format
        if file_format == 'csv':
            return read_csv_from_s3(bucket, key, engine, **kwargs)
        elif file_format == 'json':
            return read_json_from_s3(bucket, key, engine, **kwargs)
        elif file_format == 'parquet':
            return read_parquet_from_s3(bucket, key, engine, **kwargs)
        else:
            raise ValueError(f"Unsupported file format: {file_format}")
    
    except Exception as e:
        logger.error(f"Error reading file from s3://{bucket}/{key}: {str(e)}")
        logger.error(f"Erro ao ler arquivo de s3://{bucket}/{key}: {str(e)}")
        raise


def upload_file_to_s3(
    local_file_path: str,
    bucket: str,
    key: str,
    s3_client: Optional[boto3.client] = None
) -> bool:
    """
    Uploads a local file to S3.
    
    [PT-BR]
    Faz upload de um arquivo local para o S3.
    
    Args:
        local_file_path (str): Path to local file
                              Caminho do arquivo local
        bucket (str): S3 bucket name
                     Nome do bucket S3
        key (str): S3 object key
                  Chave do objeto S3
        s3_client (boto3.client, optional): S3 client instance
                                           Instância do cliente S3
    
    Returns:
        bool: True if upload successful, False otherwise
              True se o upload for bem-sucedido, False caso contrário
    """
    try:
        if s3_client is None:
            s3_client = get_s3_client()
        
        s3_client.upload_file(local_file_path, bucket, key)
        logger.info(f"Successfully uploaded {local_file_path} to s3://{bucket}/{key}")
        logger.info(f"Upload realizado com sucesso de {local_file_path} para s3://{bucket}/{key}")
        return True
    
    except Exception as e:
        logger.error(f"Error uploading file to s3://{bucket}/{key}: {str(e)}")
        logger.error(f"Erro ao fazer upload do arquivo para s3://{bucket}/{key}: {str(e)}")
        return False


def download_file_from_s3(
    bucket: str,
    key: str,
    local_file_path: str,
    s3_client: Optional[boto3.client] = None
) -> bool:
    """
    Downloads a file from S3 to local path.
    
    [PT-BR]
    Faz download de um arquivo do S3 para o caminho local.
    
    Args:
        bucket (str): S3 bucket name
                     Nome do bucket S3
        key (str): S3 object key
                  Chave do objeto S3
        local_file_path (str): Local path to save the file
                              Caminho local para salvar o arquivo
        s3_client (boto3.client, optional): S3 client instance
                                           Instância do cliente S3
    
    Returns:
        bool: True if download successful, False otherwise
              True se o download for bem-sucedido, False caso contrário
    """
    try:
        if s3_client is None:
            s3_client = get_s3_client()
        
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
        
        s3_client.download_file(bucket, key, local_file_path)
        logger.info(f"Successfully downloaded s3://{bucket}/{key} to {local_file_path}")
        logger.info(f"Download realizado com sucesso de s3://{bucket}/{key} para {local_file_path}")
        return True
    
    except Exception as e:
        logger.error(f"Error downloading file from s3://{bucket}/{key}: {str(e)}")
        logger.error(f"Erro ao fazer download do arquivo de s3://{bucket}/{key}: {str(e)}")
        return False


def delete_file_from_s3(
    bucket: str,
    key: str,
    s3_client: Optional[boto3.client] = None
) -> bool:
    """
    Deletes a file from S3.
    
    [PT-BR]
    Deleta um arquivo do S3.
    
    Args:
        bucket (str): S3 bucket name
                     Nome do bucket S3
        key (str): S3 object key
                  Chave do objeto S3
        s3_client (boto3.client, optional): S3 client instance
                                           Instância do cliente S3
    
    Returns:
        bool: True if deletion successful, False otherwise
              True se a deleção for bem-sucedida, False caso contrário
    """
    try:
        if s3_client is None:
            s3_client = get_s3_client()
        
        s3_client.delete_object(Bucket=bucket, Key=key)
        logger.info(f"Successfully deleted s3://{bucket}/{key}")
        logger.info(f"Arquivo deletado com sucesso s3://{bucket}/{key}")
        return True
    
    except Exception as e:
        logger.error(f"Error deleting file from s3://{bucket}/{key}: {str(e)}")
        logger.error(f"Erro ao deletar arquivo de s3://{bucket}/{key}: {str(e)}")
        return False


# 🚀 EXAMPLE OF USAGE / EXEMPLO DE USO

if __name__ == "__main__":
    # Example usage of S3 utilities
    # Exemplo de uso dos utilitários S3
    
    try:
        # List objects in a bucket
        # Listar objetos em um bucket
        objects = list_objects(
            prefix='data/bronze/',
            suffix='.csv',
            bucket='my-data-bucket'
        )
        print(f"Found {len(objects)} CSV files")
        print(f"Encontrados {len(objects)} arquivos CSV")
        
        # Read a CSV file from S3
        # Ler um arquivo CSV do S3
        if objects:
            df = read_csv_from_s3(
                bucket='my-data-bucket',
                key=objects[0],
                engine='pandas'
            )
            print(f"DataFrame shape: {df.shape}")
            print(f"Forma do DataFrame: {df.shape}")
        
        # Check if a file exists
        # Verificar se um arquivo existe
        exists = check_file_exists(
            bucket='my-data-bucket',
            key='data/silver/processed_data.parquet'
        )
        print(f"File exists: {exists}")
        print(f"Arquivo existe: {exists}")
        
    except Exception as e:
        print(f"Error in example: {str(e)}")
        print(f"Erro no exemplo: {str(e)}") 