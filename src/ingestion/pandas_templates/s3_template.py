"""
S3 Ingestion Template with Pandas (Functional Programming)
---------------------------------------------------------

Template for ingesting data from AWS S3 using Pandas with functional programming approach.
Supports multiple file formats and includes error handling.

[PT-BR]
Template de Ingestão S3 com Pandas (Programação Funcional)
----------------------------------------------------------

Template para ingerir dados do AWS S3 usando Pandas com abordagem de programação funcional.
Suporta múltiplos formatos de arquivo e inclui tratamento de erros.
"""

import os
import logging
from datetime import datetime
from typing import Optional, List, Dict, Any, Union, Callable
from pathlib import Path

import pandas as pd
from tenacity import retry, stop_after_attempt, wait_exponential

# Import project utilities
from utils.s3_utils import (
    get_s3_client,
    list_objects,
    read_file_from_s3,
    check_file_exists,
    upload_file_to_s3,
    get_s3_paths
)
from utils.logger import get_logger

logger = get_logger(__name__)


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10)
)
def list_source_files(
    bucket: Optional[str] = None,
    prefix: str = '',
    suffix: str = '',
    s3_client: Optional[Any] = None
) -> List[str]:
    """
    List files in source prefix with optional suffix filter.
    
    [PT-BR]
    Lista arquivos no prefixo de origem com filtro opcional de sufixo.
    
    Args:
        bucket (str, optional): S3 bucket name
                               Nome do bucket S3
        prefix (str): Source folder prefix
                     Prefixo da pasta de origem
        suffix (str): File suffix filter
                     Filtro de sufixo do arquivo
        s3_client: S3 client instance
                  Instância do cliente S3
    
    Returns:
        List[str]: List of file keys
                  Lista de chaves de arquivos
    """
    try:
        files = list_objects(
            prefix=prefix,
            suffix=suffix,
            bucket=bucket,
            s3_client=s3_client
        )
        
        logger.info(f"Found {len(files)} files in source prefix")
        logger.info(f"Encontrados {len(files)} arquivos no prefixo de origem")
        return files
    
    except Exception as e:
        logger.error(f"Error listing source files: {str(e)}")
        logger.error(f"Erro ao listar arquivos de origem: {str(e)}")
        raise


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10)
)
def read_s3_file(
    bucket: Optional[str] = None,
    key: str = '',
    file_format: str = 'auto',
    engine: str = 'pandas',
    s3_client: Optional[Any] = None,
    **kwargs
) -> pd.DataFrame:
    """
    Read file from S3 using specified engine.
    
    [PT-BR]
    Lê arquivo do S3 usando o motor especificado.
    
    Args:
        bucket (str, optional): S3 bucket name
                               Nome do bucket S3
        key (str): S3 object key
                  Chave do objeto S3
        file_format (str): File format to process
                          Formato de arquivo para processar
        engine (str): Engine to use ('pandas' or 'polars')
                     Motor a usar ('pandas' ou 'polars')
        s3_client: S3 client instance
                  Instância do cliente S3
        **kwargs: Additional arguments for reading function
                 Argumentos adicionais para função de leitura
    
    Returns:
        pd.DataFrame: DataFrame with file data
                     DataFrame com dados do arquivo
    """
    try:
        df = read_file_from_s3(
            bucket=bucket,
            key=key,
            file_format=file_format,
            engine=engine,
            **kwargs
        )
        
        logger.info(f"Successfully read file: {key}")
        logger.info(f"Arquivo lido com sucesso: {key}")
        return df
    
    except Exception as e:
        logger.error(f"Error reading file {key}: {str(e)}")
        logger.error(f"Erro ao ler arquivo {key}: {str(e)}")
        raise


def process_data(
    df: pd.DataFrame,
    custom_processor: Optional[Callable[[pd.DataFrame], pd.DataFrame]] = None
) -> pd.DataFrame:
    """
    Process the ingested data. Can be customized with a custom processor function.
    
    [PT-BR]
    Processa os dados ingeridos. Pode ser customizado com uma função processadora.
    
    Args:
        df (pd.DataFrame): Input DataFrame
                          DataFrame de entrada
        custom_processor (callable, optional): Custom processing function
                                             Função de processamento customizada
    
    Returns:
        pd.DataFrame: Processed DataFrame
                     DataFrame processado
    """
    try:
        # Apply custom processor if provided
        if custom_processor:
            df = custom_processor(df)
        
        # Add ingestion timestamp
        df['ingestion_timestamp'] = datetime.now()
        
        # Remove duplicates
        df = df.drop_duplicates()
        
        # Reset index
        df = df.reset_index(drop=True)
        
        logger.info(f"Data processed: {len(df)} rows")
        logger.info(f"Dados processados: {len(df)} linhas")
        return df
    
    except Exception as e:
        logger.error(f"Error processing data: {str(e)}")
        logger.error(f"Erro ao processar dados: {str(e)}")
        raise


def save_processed_data(
    df: pd.DataFrame,
    bucket: Optional[str] = None,
    destination_prefix: str = 'processed/',
    filename: str = '',
    format: str = 'parquet',
    s3_client: Optional[Any] = None
) -> bool:
    """
    Save processed data to S3.
    
    [PT-BR]
    Salva dados processados no S3.
    
    Args:
        df (pd.DataFrame): DataFrame to save
                          DataFrame para salvar
        bucket (str, optional): S3 bucket name
                               Nome do bucket S3
        destination_prefix (str): Destination folder prefix
                                 Prefixo da pasta de destino
        filename (str): Output filename
                       Nome do arquivo de saída
        format (str): Output format ('parquet', 'csv', 'json')
                     Formato de saída
        s3_client: S3 client instance
                  Instância do cliente S3
    
    Returns:
        bool: True if successful, False otherwise
              True se bem-sucedido, False caso contrário
    """
    try:
        # Create temporary local file
        temp_path = f"/tmp/{filename}"
        
        # Save based on format
        if format == 'parquet':
            df.to_parquet(temp_path, index=False)
        elif format == 'csv':
            df.to_csv(temp_path, index=False)
        elif format == 'json':
            df.to_json(temp_path, orient='records', lines=True)
        else:
            raise ValueError(f"Unsupported format: {format}")
        
        # Upload to S3
        s3_key = f"{destination_prefix}{filename}"
        success = upload_file_to_s3(
            local_file_path=temp_path,
            bucket=bucket,
            key=s3_key,
            s3_client=s3_client
        )
        
        # Clean up temporary file
        if os.path.exists(temp_path):
            os.remove(temp_path)
        
        if success:
            logger.info(f"Data saved to s3://{bucket}/{s3_key}")
            logger.info(f"Dados salvos em s3://{bucket}/{s3_key}")
        
        return success
    
    except Exception as e:
        logger.error(f"Error saving processed data: {str(e)}")
        logger.error(f"Erro ao salvar dados processados: {str(e)}")
        return False


def run_s3_ingestion_pipeline(
    bucket: Optional[str] = None,
    source_prefix: str = '',
    destination_prefix: str = 'processed/',
    suffix: str = '',
    file_format: str = 'auto',
    engine: str = 'pandas',
    output_format: str = 'parquet',
    custom_processor: Optional[Callable[[pd.DataFrame], pd.DataFrame]] = None,
    s3_client: Optional[Any] = None,
    **kwargs
) -> Dict[str, Any]:
    """
    Run the complete S3 ingestion pipeline using functional programming.
    
    [PT-BR]
    Executa o pipeline completo de ingestão S3 usando programação funcional.
    
    Args:
        bucket (str, optional): S3 bucket name
                               Nome do bucket S3
        source_prefix (str): Source folder prefix
                            Prefixo da pasta de origem
        destination_prefix (str): Destination folder prefix
                                 Prefixo da pasta de destino
        suffix (str): File suffix filter
                     Filtro de sufixo do arquivo
        file_format (str): File format to process
                          Formato de arquivo para processar
        engine (str): Engine to use for reading
                     Motor a usar para leitura
        output_format (str): Output format for processed data
                            Formato de saída para dados processados
        custom_processor (callable, optional): Custom data processing function
                                             Função customizada de processamento de dados
        s3_client: S3 client instance
                  Instância do cliente S3
        **kwargs: Additional arguments for file reading
                 Argumentos adicionais para leitura de arquivos
    
    Returns:
        Dict[str, Any]: Pipeline execution results
                       Resultados da execução do pipeline
    """
    start_time = datetime.now()
    results = {
        'files_processed': 0,
        'total_rows': 0,
        'errors': [],
        'processed_files': [],
        'start_time': start_time,
        'end_time': None
    }
    
    try:
        logger.info("Starting S3 ingestion pipeline (Functional)")
        logger.info("Iniciando pipeline de ingestão S3 (Funcional)")
        
        # Get S3 client if not provided
        if s3_client is None:
            s3_client = get_s3_client()
        
        # List source files
        source_files = list_source_files(
            bucket=bucket,
            prefix=source_prefix,
            suffix=suffix,
            s3_client=s3_client
        )
        
        if not source_files:
            logger.warning("No files found to process")
            logger.warning("Nenhum arquivo encontrado para processar")
            return results
        
        # Process each file
        for file_key in source_files:
            try:
                logger.info(f"Processing file: {file_key}")
                logger.info(f"Processando arquivo: {file_key}")
                
                # Read file
                df = read_s3_file(
                    bucket=bucket,
                    key=file_key,
                    file_format=file_format,
                    engine=engine,
                    s3_client=s3_client,
                    **kwargs
                )
                results['total_rows'] += len(df)
                
                # Process data
                processed_df = process_data(df, custom_processor)
                
                # Generate output filename
                base_name = Path(file_key).stem
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                output_filename = f"{base_name}_processed_{timestamp}.{output_format}"
                
                # Save processed data
                success = save_processed_data(
                    df=processed_df,
                    bucket=bucket,
                    destination_prefix=destination_prefix,
                    filename=output_filename,
                    format=output_format,
                    s3_client=s3_client
                )
                
                if success:
                    results['files_processed'] += 1
                    results['processed_files'].append(output_filename)
                
            except Exception as e:
                error_msg = f"Error processing file {file_key}: {str(e)}"
                logger.error(error_msg)
                logger.error(f"Erro ao processar arquivo {file_key}: {str(e)}")
                results['errors'].append(error_msg)
        
        results['end_time'] = datetime.now()
        duration = results['end_time'] - results['start_time']
        
        logger.info(f"Pipeline completed in {duration}")
        logger.info(f"Pipeline concluído em {duration}")
        logger.info(f"Files processed: {results['files_processed']}")
        logger.info(f"Arquivos processados: {results['files_processed']}")
        
        return results
    
    except Exception as e:
        error_msg = f"Pipeline failed: {str(e)}"
        logger.error(error_msg)
        logger.error(f"Pipeline falhou: {str(e)}")
        results['errors'].append(error_msg)
        results['end_time'] = datetime.now()
        return results


# Example custom processor functions
# Funções processadoras customizadas de exemplo

def add_metadata_processor(df: pd.DataFrame) -> pd.DataFrame:
    """
    Example custom processor that adds metadata columns.
    
    [PT-BR]
    Exemplo de processador customizado que adiciona colunas de metadados.
    """
    df['processed_at'] = datetime.now()
    df['source_system'] = 's3_ingestion'
    return df


def clean_data_processor(df: pd.DataFrame) -> pd.DataFrame:
    """
    Example custom processor that cleans data.
    
    [PT-BR]
    Exemplo de processador customizado que limpa dados.
    """
    # Remove rows with all null values
    df = df.dropna(how='all')
    
    # Fill numeric columns with 0
    numeric_columns = df.select_dtypes(include=['number']).columns
    df[numeric_columns] = df[numeric_columns].fillna(0)
    
    # Fill string columns with 'Unknown'
    string_columns = df.select_dtypes(include=['object']).columns
    df[string_columns] = df[string_columns].fillna('Unknown')
    
    return df


# 🚀 EXAMPLE OF USAGE / EXEMPLO DE USO

if __name__ == "__main__":
    # Example configuration using environment variables
    # Configuração de exemplo usando variáveis de ambiente
    
    # Load environment variables
    # Carrega variáveis de ambiente
    from dotenv import load_dotenv
    load_dotenv()
    
    # Get configuration from environment variables
    # Obtém configuração das variáveis de ambiente
    bucket = os.getenv('AWS_S3_BUCKET', 'my-data-bucket')
    source_prefix = os.getenv('AWS_S3_BRONZE_PATH', 'data/bronze/')
    destination_prefix = os.getenv('AWS_S3_SILVER_PATH', 'data/silver/')
    region = os.getenv('AWS_REGION', 'us-east-1')
    
    # Example 1: Basic pipeline
    # Exemplo 1: Pipeline básico
    results = run_s3_ingestion_pipeline(
        bucket=bucket,
        source_prefix=source_prefix,
        destination_prefix=destination_prefix,
        suffix='.csv',
        output_format='parquet'
    )
    
    # Example 2: Pipeline with custom processor
    # Exemplo 2: Pipeline com processador customizado
    results_with_processor = run_s3_ingestion_pipeline(
        bucket=bucket,
        source_prefix=source_prefix,
        destination_prefix=destination_prefix,
        suffix='.csv',
        output_format='parquet',
        custom_processor=add_metadata_processor
    )
    
    # Example 3: Pipeline with data cleaning
    # Exemplo 3: Pipeline com limpeza de dados
    results_with_cleaning = run_s3_ingestion_pipeline(
        bucket=bucket,
        source_prefix=source_prefix,
        destination_prefix=destination_prefix,
        suffix='.csv',
        output_format='parquet',
        custom_processor=clean_data_processor
    )
    
    # Print results
    # Imprimir resultados
    print(f"Files processed: {results['files_processed']}")
    print(f"Total rows: {results['total_rows']}")
    print(f"Errors: {len(results['errors'])}")
    print(f"Duration: {results['end_time'] - results['start_time']}")
    
    if results['errors']:
        print("Errors encountered:")
        for error in results['errors']:
            print(f"  - {error}") 