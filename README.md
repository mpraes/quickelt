![ChatGPT Image 23 de jun  de 2025, 08_22_36](https://github.com/user-attachments/assets/cd165cf4-f993-4507-8ab7-b507c6d3f02a)

# 🚀 The QuickELT Project 
**Template de Setup para Desenvolvedores de Engenharia de Dados**  
**Setup Template for Data Engineering Developers**

---

## 🎯 Paradigma de Programação / Programming Paradigm

Este projeto utiliza **Programação Funcional** como paradigma principal para todos os templates e utilitários. As funções são puras, imutáveis e focadas em transformação de dados, seguindo os princípios de composição e reutilização.

This project uses **Functional Programming** as the main paradigm for all templates and utilities. Functions are pure, immutable, and focused on data transformation, following composition and reusability principles.

**Características / Characteristics:**
- ✅ Funções puras e previsíveis / Pure and predictable functions
- ✅ Imutabilidade de dados / Data immutability
- ✅ Composição de funções / Function composition
- ✅ Processadores customizáveis / Customizable processors
- ✅ Sem estado compartilhado / No shared state

---

## 🧭 Índice / Table of Contents

- [🚀 The QuickELT Project](#-the-quickelt-project)
  - [🎯 Paradigma de Programação / Programming Paradigm](#-paradigma-de-programação--programming-paradigm)
  - [🧭 Índice / Table of Contents](#-índice--table-of-contents)
  - [🎯 Sobre o Projeto  / About the Project](#-sobre-o-projeto---about-the-project)
  - [✨ Funcionalidades  /  Features](#-funcionalidades----features)
  - [📁 Estrutura do Projeto  / 📁 Project Structure](#-estrutura-do-projeto----project-structure)
  - [📦 Dependências Principais / Main Dependencies](#-dependências-principais--main-dependencies)
  - [⚙️ Instalação / Installation](#️-instalação--installation)
    - [🐧 Instalação Automatizada para Linux Ubuntu](#-instalação-automatizada-para-linux-ubuntu)
    - [🖥️ Instalação Manual (Todas as Plataformas)](#️-instalação-manual-todas-as-plataformas)
  - [🐳 Infraestrutura com Docker](#-infraestrutura-com-docker)
    - [🚀 Inicialização Rápida / Quick Start](#-inicialização-rápida--quick-start)
    - [📋 Serviços Disponíveis / Available Services](#-serviços-disponíveis--available-services)
    - [🔧 Configuração Automática / Automatic Configuration](#-configuração-automática--automatic-configuration)
    - [🪣 Estrutura do Data Lake](#-estrutura-do-data-lake)
    - [🛠️ Comandos Úteis / Useful Commands](#️-comandos-úteis--useful-commands)
  - [⚙️ Executando Testes e Scripts / Running Tests and Scripts](#️-executando-testes-e-scripts--running-tests-and-scripts)
    - [🔹 Rodar todos os testes / Run all tests](#-rodar-todos-os-testes--run-all-tests)
    - [🔹 Rodar testes específicos / Run specific tests](#-rodar-testes-específicos--run-specific-tests)
    - [Rodar scripts de ingestão diretamente / Run ingestion scripts directly](#rodar-scripts-de-ingestão-diretamente--run-ingestion-scripts-directly)
    - [Limpar arquivos temporários / Clean temporary files](#limpar-arquivos-temporários--clean-temporary-files)
  - [📈 Status Atual / Current Status](#-status-atual--current-status)
    - [✅ Concluído / Completed](#-concluído--completed)
    - [🚧 Em Desenvolvimento / In Development](#-em-desenvolvimento--in-development)
    - [📝 Próximos Passos / Next Steps](#-próximos-passos--next-steps)
  - [🔐 Configuração de Segurança / Security Configuration](#-configuração-de-segurança--security-configuration)
    - [Variáveis de Ambiente Críticas / Critical Environment Variables](#variáveis-de-ambiente-críticas--critical-environment-variables)
      - [📋 Configurações Obrigatórias / Required Configurations](#-configurações-obrigatórias--required-configurations)
      - [🚀 Setup Rápido / Quick Setup](#-setup-rápido--quick-setup)
      - [🔒 Boas Práticas de Segurança / Security Best Practices](#-boas-práticas-de-segurança--security-best-practices)
  - [🔧 Exemplos de Uso / Usage Examples](#-exemplos-de-uso--usage-examples)
    - [Pipeline S3 Funcional / Functional S3 Pipeline](#pipeline-s3-funcional--functional-s3-pipeline)
    - [Processadores Customizáveis / Customizable Processors](#processadores-customizáveis--customizable-processors)
    - [🆕 Exemplo com Infraestrutura Local / Local Infrastructure Example](#-exemplo-com-infraestrutura-local--local-infrastructure-example)

---

## 🎯 Sobre o Projeto  / About the Project

Este projeto foi criado para ajudar engenheiros de dados a iniciarem seus projetos com agilidade, estrutura e boas práticas desde o início.

This project was created to help data engineers start their projects with speed, structure, and best practices from day one.

---

## ✨ Funcionalidades  /  Features

- Templates prontos para ingestão com **Pandas** e **Polars** usando programação funcional
  Ready-to-use ingestion templates with **Pandas** and **Polars** using functional programming

- Geração automática de arquivos e metadados organizados por data  
  Automatic file and metadata generation organized by date

- Logger bilíngue e estrutura de testes com Pytest  
  Bilingual logger and test structure using Pytest

- Modular, escalável e fácil de adaptar a novos contextos  
  Modular, scalable, and easy to adapt for new contexts

- **Utilitários AWS S3** com operações funcionais para pipelines em nuvem
  **AWS S3 utilities** with functional operations for cloud pipelines

- **Processadores customizáveis** para transformação de dados
  **Customizable processors** for data transformation

- **🆕 Infraestrutura Docker** completa com PostgreSQL e MinIO para desenvolvimento local
  **🆕 Complete Docker infrastructure** with PostgreSQL and MinIO for local development

- **🆕 Script de instalação automatizada** para ambientes Linux Ubuntu
  **🆕 Automated installation script** for Linux Ubuntu environments

---

## 📁 Estrutura do Projeto  / 📁 Project Structure

```bash
QUICKELT/
├── data/
│   ├── bronze/              # Dados brutos / Raw data
│   ├── silver/              # Dados tratados / Cleaned data
│   └── gold/                # Dados prontos para consumo / Analytics-ready data
│
├── infrastructure/          # 🆕 Infraestrutura Docker / Docker Infrastructure
│   ├── docker-compose.yml   # PostgreSQL + MinIO setup
│   ├── setup-quickelt-infra.sh  # Script automatizado Ubuntu / Automated Ubuntu script
│   └── init-scripts/        # Scripts de inicialização DB / DB initialization scripts
│
├── ingestion/
│   ├── pandas_templates/    # Templates com Pandas / Templates using Pandas
│   │   ├── api_template.py
│   │   ├── csv_template.py
│   │   ├── databases_template.py
│   │   ├── sharepoint_xls_template.py
│   │   ├── s3_template.py   # Template S3 funcional / Functional S3 template
│   │   └── web_scraping_template.py
│   │
│   └── polars_templates/    # Templates com Polars / Templates using Polars
│       ├── api_template.py
│       ├── csv_template.py
│       ├── databases_template.py
│       ├── sharepoint_xls_template.py
│       ├── s3_template.py   # Template S3 funcional / Functional S3 template
│       └── web_scraping_template.py
│
├── metadata/                # Metadados das ingestões / Ingestion metadata
│
├── tests/
│   ├── conftest.py
│   ├── test_ingestion_pandas.py
│   ├── test_ingestion_polars.py
│   ├── test_ingestion_databases_pandas_functions.py
│   └── test_ingestion_databases_polars_functions.py
│
├── transformation/
│   └── to_silver/
│       ├── cleaning_template_duckdb.py   # Template de limpeza com DuckDB / DuckDB cleaning template
│       ├── cleaning_template_pandas.py   # Template de limpeza com Pandas / Pandas cleaning template
│       └── cleaning_template_polars.py   # Template de limpeza com Polars / Polars cleaning template
│
├── utils/
│   ├── logger.py             # Logger bilíngue / Bilingual logger
│   └── s3_utils.py           # Utilitários AWS S3 / AWS S3 utilities
│
├── .env                      # Variáveis de ambiente / Environment variables
├── CHECKLIST.md             # Checklist de boas práticas / Best Practices Checklist
├── README.md
└── requirements.txt
```

---

## 📦 Dependências Principais / Main Dependencies

- **Frameworks de Dados / Data Frameworks**
  - pandas>=2.2.2
  - polars>=0.20.28
  - duckdb>=0.9.2

- **Conectores de Banco de Dados / Database Connectors**
  - sqlalchemy>=2.0.30
  - psycopg2-binary>=2.9.9 (PostgreSQL)
  - pymysql>=1.1.0 (MySQL)
  - cx_Oracle>=8.3.0 (Oracle)
  - pyodbc>=5.0.1 (MS SQL Server)

- **Integração Microsoft / Microsoft Integration**
  - msal>=1.26.0
  - openpyxl>=3.1.2

- **AWS S3 Integration**
  - boto3>=1.34.0
  - botocore>=1.34.0

- **Web Scraping & APIs**
  - requests>=2.31.0
  - beautifulsoup4>=4.12.3
  - lxml>=4.9.3

- **Utilitários / Utilities**
  - python-dotenv>=1.0.1
  - tenacity>=8.2.3
  - tqdm>=4.66.2

- **Formatos de Arquivo / File Formats**
  - pyarrow>=15.0.1
  - fastparquet>=2024.2.0

- **Testes / Testing**
  - pytest>=8.2.2

- **🆕 Infraestrutura / Infrastructure**
  - Docker e Docker Compose
  - PostgreSQL 15 (via Docker)
  - MinIO (S3-compatible via Docker)

---

## ⚙️ Instalação / Installation

### 🐧 Instalação Automatizada para Linux Ubuntu

**Para ambientes Linux Ubuntu, use nosso script de instalação completa:**

```bash
# 1. Baixar o script de instalação
wget https://raw.githubusercontent.com/mpraes/quickelt/main/infrastructure/setup-quickelt-infra.sh

# 2. Dar permissão de execução
chmod +x setup-quickelt-infra.sh

# 3. Executar instalação completa
./setup-quickelt-infra.sh
```

**O script automaticamente:**
- ✅ Verifica e atualiza o sistema Ubuntu/Debian
- ✅ Instala Docker e Docker Compose
- ✅ Configura Python 3.8+ e ambiente virtual
- ✅ Clona o projeto para `/opt/quickelt`
- ✅ Instala todas as dependências Python
- ✅ Configura infraestrutura Docker (PostgreSQL + MinIO)
- ✅ Cria arquivo `.env` com configurações locais
- ✅ Configura bucket MinIO com estrutura bronze/silver/gold
- ✅ Executa testes automatizados
- ✅ Cria script de inicialização `start-quickelt.sh`

**Após instalação automatizada:**
```bash
# Reiniciar sessão para aplicar permissões Docker
# Restart session to apply Docker permissions
exit
# (faça login novamente / log in again)

# Iniciar ambiente QuickELT
/opt/quickelt/start-quickelt.sh

# Acessar interfaces web
# PostgreSQL: localhost:5432 (user: quickelt_user, pass: quickelt_password)
# MinIO Console: http://localhost:9001 (user: minioadmin, pass: minioadmin123)
# MinIO API: http://localhost:9000
```

### 🖥️ Instalação Manual (Todas as Plataformas)

**Para Windows, macOS ou instalação customizada:**

1️⃣ **Clone o repositório / Clone the repository**
```bash
git clone https://github.com/mpraes/quickelt.git
cd quickelt
```

2️⃣ **Crie e ative um ambiente virtual / Create and activate a virtual environment**
```bash	
python -m venv .venv
source .venv/bin/activate      # Linux/macOS
.venv\Scripts\activate         # Windows
```

3️⃣ **Instale as dependências / Install dependencies**
```bash	
pip install -r requirements.txt
```

4️⃣ **Configure variáveis de ambiente / Configure environment variables**

**Opção 1: Script Interativo (Recomendado) / Interactive Script (Recommended)**
```bash
python setup_env.py
```

**Opção 2: Copiar Arquivo de Exemplo / Copy Example File**
```bash
cp config.env.example .env
```

5️⃣ **Configure as variáveis críticas no arquivo .env / Configure critical variables in the .env file:**
```bash
# AWS S3 Configuration
AWS_ACCESS_KEY_ID=your_aws_access_key_id_here
AWS_SECRET_ACCESS_KEY=your_aws_secret_access_key_here
AWS_REGION=us-east-1
AWS_S3_BUCKET=your-bucket-name

# Database Configuration (choose your database)
POSTGRES_HOST=localhost
POSTGRES_USERNAME=your_username
POSTGRES_PASSWORD=your_password
POSTGRES_DATABASE=quickelt_db

# SharePoint Configuration (if using)
AZURE_TENANT_ID=your_tenant_id
AZURE_CLIENT_ID=your_client_id
AZURE_CLIENT_SECRET=your_client_secret
```

6️⃣ **Execute os testes automáticos / Run automatic tests**
```bash	
pytest
```

---

## 🐳 Infraestrutura com Docker

O QuickELT agora inclui uma infraestrutura Docker completa para desenvolvimento local, permitindo que você trabalhe com PostgreSQL e MinIO (S3-compatible) sem precisar instalar ou configurar esses serviços manualmente.

QuickELT now includes a complete Docker infrastructure for local development, allowing you to work with PostgreSQL and MinIO (S3-compatible) without needing to manually install or configure these services.

### 🚀 Inicialização Rápida / Quick Start

```bash
# Navegar para o diretório de infraestrutura
cd infrastructure

# Iniciar todos os serviços
docker-compose up -d

# Verificar status dos containers
docker-compose ps

# Ver logs em tempo real
docker-compose logs -f
```

### 📋 Serviços Disponíveis / Available Services

| Serviço / Service | Porta / Port | Credenciais / Credentials | Uso / Usage |
|------------------|--------------|---------------------------|-------------|
| **PostgreSQL** | `5432` | user: `quickelt_user`<br>pass: `quickelt_password`<br>db: `quickelt_db` | Templates `database_template.py` |
| **MinIO Console** | `9001` | user: `minioadmin`<br>pass: `minioadmin123` | Gerenciar buckets S3<br>Manage S3 buckets |
| **MinIO API** | `9000` | Access Key: `minioadmin`<br>Secret: `minioadmin123` | Templates `s3_template.py` |

### 🔧 Configuração Automática / Automatic Configuration

O arquivo `.env` é automaticamente configurado para usar a infraestrutura local:

The `.env` file is automatically configured to use the local infrastructure:

```bash
# Configuração para desenvolvimento local com Docker
# Configuration for local development with Docker
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USERNAME=quickelt_user
POSTGRES_PASSWORD=quickelt_password
POSTGRES_DATABASE=quickelt_db

AWS_ENDPOINT_URL=http://localhost:9000  # MinIO local
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin123
AWS_S3_BUCKET=quickelt-data
```

### 🪣 Estrutura do Data Lake

O bucket MinIO é automaticamente configurado com a estrutura de data lake:

The MinIO bucket is automatically configured with the data lake structure:

```
quickelt-data/
├── bronze/    # Dados brutos / Raw data
├── silver/    # Dados tratados / Cleaned data
└── gold/      # Dados prontos para análise / Analytics-ready data
```

### 🛠️ Comandos Úteis / Useful Commands

```bash
# Parar todos os serviços
docker-compose down

# Parar e remover volumes (RESET completo)
docker-compose down -v

# Reiniciar apenas um serviço
docker-compose restart postgres
docker-compose restart minio

# Acessar PostgreSQL via linha de comando
docker exec -it quickelt-postgres psql -U quickelt_user -d quickelt_db

# Ver uso de recursos
docker stats quickelt-postgres quickelt-minio
```

---

## ⚙️ Executando Testes e Scripts / Running Tests and Scripts

Este projeto já possui um **Makefile** configurado para facilitar a execução dos testes e dos scripts de ingestão.

This project already includes a **Makefile** configured to make it easy to run tests and ingestion scripts.

### 🔹 Rodar todos os testes / Run all tests

```bash
make test
```

### 🔹 Rodar testes específicos / Run specific tests

Rodar apenas os testes de ingestion de banco de dados com Pandas / Run only the database ingestion tests with Pandas:

```bash
make test_pandas_databases
```

Rodar apenas os testes de ingestion de banco de dados com Polars / Run only the database ingestion tests with Polars:

```bash
make test_polars_databases
```

Rodar apenas os testes gerais dos templates Pandas / Run only the general tests for the Pandas templates:

```bash
make test_pandas_templates
```

Rodar apenas os testes gerais dos templates Polars / Run only the general tests for the Polars templates:

```bash
make test_polars_templates
```

### Rodar scripts de ingestão diretamente / Run ingestion scripts directly

Rodar ingestion de banco usando Pandas / Run database ingestion with Pandas:

```bash
make run_pandas_database
```

Rodar ingestion de banco usando Polars / Run database ingestion with Polars:

```bash
make run_polars_database
```

### Limpar arquivos temporários / Clean temporary files

```bash
make clean
```	

Caso precise de ajuda com os comandos disponíveis / If you need help with the available commands:

```bash
make help
```

---

## 📈 Status Atual / Current Status

### ✅ Concluído / Completed
- [x] Templates de ingestão com Pandas e Polars (programação funcional)
- [x] Logger bilíngue para rastreamento dos processos
- [x] Estrutura completa de testes unitários com Pytest
- [x] Makefile configurado para rodar testes e scripts facilmente
- [x] Templates de limpeza de dados (Pandas)
- [x] Templates de limpeza de dados (Polars e DuckDB)
- [x] Estrutura modularizada e profissional de ingestão
- [x] Documentação bilingue (PT/EN) em todos os templates
- [x] Suporte a múltiplas fontes de dados (APIs, bancos, arquivos)
- [x] Tratamento de datas e fusos horários
- [x] Padronização de valores monetários
- [x] Deduplicação de dados
- [x] Enriquecimento de dados via joins
- [x] Validação de integridade referencial
- [x] Utilitários AWS S3 para operações em nuvem (programação funcional)
- [x] Processadores customizáveis para transformação de dados
- [x] **🆕 Infraestrutura Docker completa (PostgreSQL + MinIO)**
- [x] **🆕 Script de instalação automatizada para Ubuntu Linux**
- [x] **🆕 Configuração automática de ambiente de desenvolvimento local**
- [x] **🆕 Data lake estruturado com camadas bronze/silver/gold**
- [x] **🆕 Script de inicialização rápida do ambiente**

### 🚧 Em Desenvolvimento / In Development
- [ ] Templates de transformação avançada (pós-pré-processamento para silver)
- [ ] Adicionar integração contínua (CI/CD) com GitHub Actions
- [ ] Implementar cache de dados para otimização de performance
- [ ] Adicionar suporte a mais formatos de arquivo (Excel, JSON, XML)
- [ ] Desenvolver dashboard de monitoramento de pipelines
- [ ] Implementar sistema de versionamento de schemas
- [ ] Adicionar suporte a processamento distribuído
- [ ] **🆕 Script de instalação para Windows e macOS**
- [ ] **🆕 Interface web para gerenciamento de pipelines**
- [ ] **🆕 Monitoramento automático de health checks dos serviços**

### 📝 Próximos Passos / Next Steps
1. **Infraestrutura e DevOps**
   - Script de instalação para Windows/macOS
   - Configurar CI/CD com GitHub Actions
   - Interface web para monitoramento
   - Health checks automáticos

2. **Transformação Avançada**
   - Desenvolver templates para transformações complexas
   - Implementar validações de qualidade de dados
   - Adicionar suporte a agregações e métricas

3. **Performance e Escalabilidade**
   - Implementar cache de dados
   - Otimizar queries DuckDB
   - Adicionar suporte a processamento distribuído

4. **Documentação e Testes**
   - Expandir documentação com exemplos práticos
   - Aumentar cobertura de testes
   - Adicionar documentação de API

Contribuições são bem-vindas!
Contributions are welcome!

Solicite adição de contribuidor, e com isso crie uma branch e abra um pull request com sugestões, melhorias ou novos templates. Pode também abrir issues ou até entrar em contato comigo com sugestões.
Feel free to request contributor access, create a branch, and open a pull request with suggestions, improvements, or new templates. You can also open issues or contact me directly with suggestions.

---

## 🔐 Configuração de Segurança / Security Configuration

### Variáveis de Ambiente Críticas / Critical Environment Variables

O projeto QuickELT utiliza variáveis de ambiente para todas as configurações sensíveis. **NUNCA** commite o arquivo `.env` real no controle de versão.

The QuickELT project uses environment variables for all sensitive configurations. **NEVER** commit the actual `.env` file to version control.

#### 📋 Configurações Obrigatórias / Required Configurations

**🆕 Desenvolvimento Local (Docker):**
```bash
# Configuração automática via script de instalação
# Automatic configuration via installation script
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USERNAME=quickelt_user
POSTGRES_PASSWORD=quickelt_password
POSTGRES_DATABASE=quickelt_db

AWS_ENDPOINT_URL=http://localhost:9000  # MinIO local
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin123
AWS_S3_BUCKET=quickelt-data
```

**AWS S3 (Produção):**
- `AWS_ACCESS_KEY_ID` - Chave de acesso AWS
- `AWS_SECRET_ACCESS_KEY` - Chave secreta AWS
- `AWS_REGION` - Região AWS (padrão: us-east-1)
- `AWS_S3_BUCKET` - Nome do bucket S3

**Banco de Dados Externo / External Database:**
- `POSTGRES_HOST` / `MYSQL_HOST` / `ORACLE_HOST` - Host do banco
- `POSTGRES_USERNAME` / `MYSQL_USERNAME` / `ORACLE_USERNAME` - Usuário
- `POSTGRES_PASSWORD` / `MYSQL_PASSWORD` / `ORACLE_PASSWORD` - Senha
- `POSTGRES_DATABASE` / `MYSQL_DATABASE` - Nome do banco

**SharePoint:**
- `AZURE_TENANT_ID` - ID do tenant Azure
- `AZURE_CLIENT_ID` - ID do cliente Azure
- `AZURE_CLIENT_SECRET` - Segredo do cliente Azure

#### 🚀 Setup Rápido / Quick Setup

```bash
# 1. Usar script automatizado Ubuntu (RECOMENDADO)
./infrastructure/setup-quickelt-infra.sh

# 2. Ou usar script interativo manual
python setup_env.py

# 3. Ou copiar arquivo de exemplo
cp config.env.example .env

# 4. Editar configurações
nano .env

# 5. Verificar se .env está no .gitignore
grep .env .gitignore
```

#### 🔒 Boas Práticas de Segurança / Security Best Practices

1. **Use IAM Roles** em produção em vez de chaves de acesso
2. **Rotacione credenciais** regularmente
3. **Use serviços de gerenciamento de segredos** (AWS Secrets Manager, Azure Key Vault)
4. **Configure diferentes .env** para diferentes ambientes
5. **Monitore logs** de acesso e uso de credenciais
6. **🆕 Use a infraestrutura Docker local** para desenvolvimento
7. **🆕 Mantenha credenciais de produção separadas** das de desenvolvimento

---

## 🔧 Exemplos de Uso / Usage Examples

### Pipeline S3 Funcional / Functional S3 Pipeline

```python
from ingestion.pandas_templates.s3_template import run_s3_ingestion_pipeline

# Pipeline básico com MinIO local
results = run_s3_ingestion_pipeline(
    bucket='quickelt-data',  # Bucket configurado automaticamente
    source_prefix='data/bronze/',
    destination_prefix='data/silver/',
    suffix='.csv',
    output_format='parquet'
)

# Pipeline com processador customizado
def my_processor(df):
    df['processed_at'] = datetime.now()
    return df

results = run_s3_ingestion_pipeline(
    bucket='quickelt-data',
    custom_processor=my_processor
)
```

### Processadores Customizáveis / Customizable Processors

```python
# Exemplo de processador para limpeza de dados
def clean_data_processor(df):
    # Remove linhas com todos os valores nulos
    df = df.dropna(how='all')
    
    # Preenche valores numéricos com 0
    numeric_cols = df.select_dtypes(include=['number']).columns
    df[numeric_cols] = df[numeric_cols].fillna(0)
    
    return df

# Aplicar processador no pipeline
results = run_s3_ingestion_pipeline(
    custom_processor=clean_data_processor
)
```

### 🆕 Exemplo com Infraestrutura Local / Local Infrastructure Example

```python
# Usando PostgreSQL local via Docker
from ingestion.pandas_templates.databases_template import run_postgres_ingestion

# Configuração automática via .env
results = run_postgres_ingestion(
    query="SELECT * FROM sales_data WHERE created_at >= '2024-01-01'",
    destination_prefix='bronze/sales/',
    output_format='parquet'
)

# Pipeline completo: PostgreSQL -> MinIO
from utils.s3_utils import upload_to_s3

# 1. Extrair do PostgreSQL
df = run_postgres_ingestion(query="SELECT * FROM users")

# 2. Carregar para MinIO (data lake local)
upload_to_s3(
    df=df,
    bucket='quickelt-data',
    key='bronze/users/users_2024.parquet'
)
```

---

**Distribuído sob a licença MIT.**  
**Distributed under the MIT license.**

**Use livre para fins comerciais ou educacionais.**  
**Free to use for commercial or educational purposes.**

**🚀 Happy coding with QuickELT!**

---

Visite o [CHECKLIST.md](CHECKLIST.md) para mais informações sobre boas práticas e orientações.  
Visit the [CHECKLIST.md](CHECKLIST.md) for more information on best practices and guidelines.


