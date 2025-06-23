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
    - [🔹 Rodar testes específicos / Run specific tests](#-rodar-testes-específicos--run-specific-tests)
    - [Rodar scripts de ingestão diretamente / Run ingestion scripts directly](#rodar-scripts-de-ingestão-diretamente--run-ingestion-scripts-directly)
    - [Limpar arquivos temporários / Clean temporary files](#limpar-arquivos-temporários--clean-temporary-files)
- [📋 Atualização para "📈 Status Atual / Current Status"](#-atualização-para--status-atual--current-status)
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

---

## 📁 Estrutura do Projeto  / 📁 Project Structure

```bash
QUICKELT/
├── data/
│   ├── bronze/              # Dados brutos / Raw data
│   ├── silver/              # Dados tratados / Cleaned data
│   └── gold/                # Dados prontos para consumo / Analytics-ready data
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

---

⚙️ Instalação / Installation
1️⃣ Clone o repositório / Clone the repository
```bash
git clone https://github.com/mpraes/quickelt.git
cd quickelt
```	
2️⃣ Crie e ative um ambiente virtual / Create and activate a virtual environment
```bash	
python -m venv .venv
source .venv/bin/activate      # Linux/macOS
.venv\Scripts\activate         # Windows
```	
3️⃣ Instale as dependências / Install dependencies
```bash	
pip install -r requirements.txt
```	
4️⃣ (OPCIONAL/OPTIONAL) Configure variáveis de ambiente criando o arquivo .env no diretório raiz. No arquivo .env do repositório tem os exemplos. Use o comando abaixo caso precise. / Configure environment variables by creating a .env file in the root directory. The .env file in the repository contains examples. Use the command below to create the file in case of need.

**Opção 1: Script Interativo (Recomendado) / Interactive Script (Recommended)**
```bash
python setup_env.py
```

**Opção 2: Copiar Arquivo de Exemplo / Copy Example File**
```bash
cp config.env.example .env
```

5️⃣ Configure as variáveis críticas no arquivo .env / Configure critical variables in the .env file:
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

6️⃣ (OPCIONAL/OPTIONAL) Execute os testes automáticos / Run automatic tests
```bash	
pytest
```	
---

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

Visite o [CHECKLIST.md](CHECKLIST.md) para mais informações sobre boas práticas e orientações.  
Visit the [CHECKLIST.md](CHECKLIST.md) for more information on best practices and guidelines.


---

# 📋 Atualização para "📈 Status Atual / Current Status"

Sugestão de atualização para refletir melhor o que você já conquistou e o que pode fazer nos próximos passos:

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

### 🚧 Em Desenvolvimento / In Development
- [ ] Templates de transformação avançada (pós-pré-processamento para silver)
- [ ] Adicionar integração contínua (CI/CD) com GitHub Actions
- [ ] Criar imagens Docker para ambientes de execução padronizados
- [ ] Implementar cache de dados para otimização de performance
- [ ] Adicionar suporte a mais formatos de arquivo (Excel, JSON, XML)
- [ ] Desenvolver dashboard de monitoramento de pipelines
- [ ] Implementar sistema de versionamento de schemas
- [ ] Adicionar suporte a processamento distribuído

### 📝 Próximos Passos / Next Steps
1. **Transformação Avançada**
   - Desenvolver templates para transformações complexas
   - Implementar validações de qualidade de dados
   - Adicionar suporte a agregações e métricas

2. **DevOps e Infraestrutura**
   - Configurar CI/CD com GitHub Actions
   - Criar Dockerfile e docker-compose
   - Implementar monitoramento e alertas

3. **Documentação e Testes**
   - Expandir documentação com exemplos práticos
   - Aumentar cobertura de testes
   - Adicionar documentação de API

4. **Performance e Escalabilidade**
   - Implementar cache de dados
   - Otimizar queries DuckDB
   - Adicionar suporte a processamento distribuído

Contribuições são bem-vindas!
Contributions are welcome!

Solicite adição de contribuidor, e com isso crie uma branch e abra um pull request com sugestões, melhorias ou novos templates. Pode também abrir issues ou até entrar em contato comigo com sugestões.
Feel free to request contributor access, create a branch, and open a pull request with suggestions, improvements, or new templates. You can also open issues or contact me directly with suggestions.

Distribuído sob a licença MIT.
Distributed under the MIT license.
Use livre para fins comerciais ou educacionais.
Free to use for commercial or educational purposes.

## 🔐 Configuração de Segurança / Security Configuration

### Variáveis de Ambiente Críticas / Critical Environment Variables

O projeto QuickELT utiliza variáveis de ambiente para todas as configurações sensíveis. **NUNCA** commite o arquivo `.env` real no controle de versão.

The QuickELT project uses environment variables for all sensitive configurations. **NEVER** commit the actual `.env` file to version control.

#### 📋 Configurações Obrigatórias / Required Configurations

**AWS S3:**
- `AWS_ACCESS_KEY_ID` - Chave de acesso AWS
- `AWS_SECRET_ACCESS_KEY` - Chave secreta AWS
- `AWS_REGION` - Região AWS (padrão: us-east-1)
- `AWS_S3_BUCKET` - Nome do bucket S3

**Banco de Dados / Database:**
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
# 1. Usar script interativo (recomendado)
python setup_env.py

# 2. Ou copiar arquivo de exemplo
cp config.env.example .env

# 3. Editar configurações
nano .env

# 4. Verificar se .env está no .gitignore
grep .env .gitignore
```

#### 🔒 Boas Práticas de Segurança / Security Best Practices

1. **Use IAM Roles** em produção em vez de chaves de acesso
2. **Rotacione credenciais** regularmente
3. **Use serviços de gerenciamento de segredos** (AWS Secrets Manager, Azure Key Vault)
4. **Configure diferentes .env** para diferentes ambientes
5. **Monitore logs** de acesso e uso de credenciais

## 🔧 Exemplos de Uso / Usage Examples

### Pipeline S3 Funcional / Functional S3 Pipeline

```python
from ingestion.pandas_templates.s3_template import run_s3_ingestion_pipeline

# Pipeline básico
results = run_s3_ingestion_pipeline(
    bucket='my-bucket',
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
    bucket='my-bucket',
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


