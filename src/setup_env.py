#!/usr/bin/env python3
"""
QuickELT Environment Setup Script
=================================

This script helps you set up your environment variables for the QuickELT project.
It will create a .env file with your configurations.

[PT-BR]
Script de Configuração de Ambiente QuickELT
===========================================

Este script ajuda você a configurar suas variáveis de ambiente para o projeto QuickELT.
Ele criará um arquivo .env com suas configurações.
"""

import os
import sys
from pathlib import Path


def get_user_input(prompt: str, default: str = '', is_secret: bool = False) -> str:
    """
    Get user input with optional default value.
    
    [PT-BR]
    Obtém entrada do usuário com valor padrão opcional.
    """
    if default:
        prompt = f"{prompt} (default: {default}): "
    else:
        prompt = f"{prompt}: "
    
    if is_secret:
        import getpass
        value = getpass.getpass(prompt)
    else:
        value = input(prompt)
    
    return value if value else default


def setup_aws_config() -> dict:
    """
    Setup AWS S3 configuration.
    
    [PT-BR]
    Configura configuração AWS S3.
    """
    print("\n🔐 AWS S3 Configuration / Configuração AWS S3")
    print("=" * 50)
    
    config = {}
    
    config['AWS_ACCESS_KEY_ID'] = get_user_input(
        "Enter your AWS Access Key ID",
        is_secret=True
    )
    
    config['AWS_SECRET_ACCESS_KEY'] = get_user_input(
        "Enter your AWS Secret Access Key",
        is_secret=True
    )
    
    config['AWS_REGION'] = get_user_input(
        "Enter AWS Region",
        "us-east-1"
    )
    
    config['AWS_S3_BUCKET'] = get_user_input(
        "Enter S3 Bucket name",
        "quickelt-data-bucket"
    )
    
    return config


def setup_database_config() -> dict:
    """
    Setup database configuration.
    
    [PT-BR]
    Configura configuração de banco de dados.
    """
    print("\n🗄️ Database Configuration / Configuração de Banco de Dados")
    print("=" * 50)
    
    config = {}
    
    # Ask which database to configure
    db_type = get_user_input(
        "Which database do you want to configure? (postgres/mysql/oracle/sqlserver/none)",
        "postgres"
    ).lower()
    
    if db_type == 'none':
        return config
    
    if db_type == 'postgres':
        config['POSTGRES_HOST'] = get_user_input("PostgreSQL Host", "localhost")
        config['POSTGRES_PORT'] = get_user_input("PostgreSQL Port", "5432")
        config['POSTGRES_DATABASE'] = get_user_input("PostgreSQL Database", "quickelt_db")
        config['POSTGRES_USERNAME'] = get_user_input("PostgreSQL Username")
        config['POSTGRES_PASSWORD'] = get_user_input("PostgreSQL Password", is_secret=True)
        config['POSTGRES_SCHEMA'] = get_user_input("PostgreSQL Schema", "public")
    
    elif db_type == 'mysql':
        config['MYSQL_HOST'] = get_user_input("MySQL Host", "localhost")
        config['MYSQL_PORT'] = get_user_input("MySQL Port", "3306")
        config['MYSQL_DATABASE'] = get_user_input("MySQL Database", "quickelt_db")
        config['MYSQL_USERNAME'] = get_user_input("MySQL Username")
        config['MYSQL_PASSWORD'] = get_user_input("MySQL Password", is_secret=True)
    
    elif db_type == 'oracle':
        config['ORACLE_HOST'] = get_user_input("Oracle Host", "localhost")
        config['ORACLE_PORT'] = get_user_input("Oracle Port", "1521")
        config['ORACLE_SERVICE_NAME'] = get_user_input("Oracle Service Name", "orcl")
        config['ORACLE_USERNAME'] = get_user_input("Oracle Username")
        config['ORACLE_PASSWORD'] = get_user_input("Oracle Password", is_secret=True)
    
    elif db_type == 'sqlserver':
        config['SQLSERVER_HOST'] = get_user_input("SQL Server Host", "localhost")
        config['SQLSERVER_PORT'] = get_user_input("SQL Server Port", "1433")
        config['SQLSERVER_DATABASE'] = get_user_input("SQL Server Database", "quickelt_db")
        config['SQLSERVER_USERNAME'] = get_user_input("SQL Server Username")
        config['SQLSERVER_PASSWORD'] = get_user_input("SQL Server Password", is_secret=True)
    
    return config


def setup_sharepoint_config() -> dict:
    """
    Setup SharePoint configuration.
    
    [PT-BR]
    Configura configuração do SharePoint.
    """
    print("\n📄 SharePoint Configuration / Configuração SharePoint")
    print("=" * 50)
    
    config = {}
    
    use_sharepoint = get_user_input(
        "Do you want to configure SharePoint? (y/n)",
        "n"
    ).lower()
    
    if use_sharepoint != 'y':
        return config
    
    config['AZURE_TENANT_ID'] = get_user_input("Azure Tenant ID")
    config['AZURE_CLIENT_ID'] = get_user_input("Azure Client ID")
    config['AZURE_CLIENT_SECRET'] = get_user_input("Azure Client Secret", is_secret=True)
    config['SHAREPOINT_SITE_URL'] = get_user_input("SharePoint Site URL")
    
    return config


def setup_api_config() -> dict:
    """
    Setup API configuration.
    
    [PT-BR]
    Configura configuração de API.
    """
    print("\n🌐 API Configuration / Configuração de API")
    print("=" * 50)
    
    config = {}
    
    use_api = get_user_input(
        "Do you want to configure API access? (y/n)",
        "n"
    ).lower()
    
    if use_api != 'y':
        return config
    
    config['API_BASE_URL'] = get_user_input("API Base URL", "https://api.example.com")
    config['API_KEY'] = get_user_input("API Key", is_secret=True)
    config['API_SECRET'] = get_user_input("API Secret", is_secret=True)
    config['API_TIMEOUT'] = get_user_input("API Timeout (seconds)", "30")
    
    return config


def setup_logging_config() -> dict:
    """
    Setup logging configuration.
    
    [PT-BR]
    Configura configuração de logs.
    """
    print("\n📝 Logging Configuration / Configuração de Logs")
    print("=" * 50)
    
    config = {}
    
    config['LOG_LEVEL'] = get_user_input("Log Level (DEBUG/INFO/WARNING/ERROR)", "INFO")
    config['LOG_FILE_PATH'] = get_user_input("Log File Path", "logs/quickelt.log")
    
    return config


def create_env_file(config: dict, env_file_path: str = '.env'):
    """
    Create .env file with configurations.
    
    [PT-BR]
    Cria arquivo .env com configurações.
    """
    try:
        with open(env_file_path, 'w') as f:
            f.write("# QuickELT Environment Configuration\n")
            f.write("# Generated by setup_env.py\n\n")
            
            for key, value in config.items():
                if value:  # Only write non-empty values
                    f.write(f"{key}={value}\n")
        
        print(f"\n✅ Environment file created successfully: {env_file_path}")
        print(f"✅ Arquivo de ambiente criado com sucesso: {env_file_path}")
        
    except Exception as e:
        print(f"\n❌ Error creating environment file: {str(e)}")
        print(f"❌ Erro ao criar arquivo de ambiente: {str(e)}")
        return False
    
    return True


def main():
    """
    Main setup function.
    
    [PT-BR]
    Função principal de configuração.
    """
    print("🚀 QuickELT Environment Setup")
    print("=" * 40)
    print("This script will help you configure your environment variables.")
    print("Este script ajudará você a configurar suas variáveis de ambiente.")
    print()
    
    # Check if .env already exists
    if os.path.exists('.env'):
        overwrite = get_user_input(
            "File .env already exists. Overwrite? (y/n)",
            "n"
        ).lower()
        
        if overwrite != 'y':
            print("Setup cancelled. / Configuração cancelada.")
            return
    
    # Collect configurations
    config = {}
    
    # AWS Configuration
    config.update(setup_aws_config())
    
    # Database Configuration
    config.update(setup_database_config())
    
    # SharePoint Configuration
    config.update(setup_sharepoint_config())
    
    # API Configuration
    config.update(setup_api_config())
    
    # Logging Configuration
    config.update(setup_logging_config())
    
    # Add some default configurations
    config.update({
        'ENVIRONMENT': 'development',
        'DEFAULT_ENGINE': 'pandas',
        'DEFAULT_OUTPUT_FORMAT': 'parquet',
        'BATCH_SIZE': '10000',
        'MAX_WORKERS': '4'
    })
    
    # Create .env file
    success = create_env_file(config)
    
    if success:
        print("\n🎉 Setup completed successfully!")
        print("🎉 Configuração concluída com sucesso!")
        print("\n📋 Next steps / Próximos passos:")
        print("1. Review your .env file")
        print("2. Run: pip install -r requirements.txt")
        print("3. Run: pytest")
        print("4. Start using QuickELT!")
        
        print("\n📋 Próximos passos:")
        print("1. Revise seu arquivo .env")
        print("2. Execute: pip install -r requirements.txt")
        print("3. Execute: pytest")
        print("4. Comece a usar o QuickELT!")


if __name__ == "__main__":
    main() 