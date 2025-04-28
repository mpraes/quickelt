"""
Conftest.py para configuração global dos testes

Este módulo define configurações globais para os testes automáticos,
utilizando Pytest Fixtures.

ORIENTAÇÕES:
- Carrega o arquivo .env automaticamente antes dos testes.
- Garante que os diretórios de saída (data/bronze/ e metadata/) existem.
- (Opcional) Limpa arquivos antigos para evitar lixo de execuções anteriores.

INSTRUCTIONS:
- Automatically loads .env file before tests.
- Ensures output directories (data/bronze/ and metadata/) exist.
- (Optional) Cleans old files to avoid leftover files from previous runs.

Dependências / Dependencies:
- pytest
- os
- shutil
- python-dotenv
"""

import os
import shutil
import pytest
from dotenv import load_dotenv

@pytest.fixture(autouse=True, scope="session")
def setup_environment():
    """
    Fixture para configurar ambiente de testes:
    - Carrega variáveis de ambiente do .env
    - Garante a existência dos diretórios bronze e metadata
    - (Opcional) Limpa arquivos antigos
    """
    print("\nConfigurando ambiente de testes / Setting up test environment")

    # Carregar variáveis do .env
    load_dotenv()
    print("Arquivo .env carregado / .env file loaded")

    # Diretórios que devem existir
    directories = [
        "./data/bronze",
        "./metadata"
    ]

    for directory in directories:
        os.makedirs(directory, exist_ok=True)
        print(f"Diretório garantido: {directory}")

    # (Opcional) Limpar arquivos antigos
    clean_directories(directories)

def clean_directories(directories):
    """
    Remove todos os arquivos dos diretórios especificados.
    Remove all files from the specified directories.
    """
    for directory in directories:
        for root, dirs, files in os.walk(directory):
            for file in files:
                file_path = os.path.join(root, file)
                try:
                    os.remove(file_path)
                    print(f"Arquivo removido: {file_path}")
                except Exception as e:
                    print(f"Erro ao remover {file_path}: {e}")
