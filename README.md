# 🚀 The Pipeline Craft  
**Template de Setup para Desenvolvedores de Engenharia de Dados**  
**Setup Template for Data Engineering Developers**

---

## 🧭 Índice / Table of Contents

- [🚀 The Pipeline Craft](#-the-pipeline-craft)
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
  - [🛠️ Próximos Passos / Next Steps](#️-próximos-passos--next-steps)

---

## 🎯 Sobre o Projeto  / About the Project

Este projeto foi criado para ajudar engenheiros de dados a iniciarem seus projetos com agilidade, estrutura e boas práticas desde o início.

This project was created to help data engineers start their projects with speed, structure, and best practices from day one.

---

## ✨ Funcionalidades  /  Features

- Templates prontos para ingestão com **Pandas** e **Polars**  
  Ready-to-use ingestion templates with **Pandas** and **Polars**

- Geração automática de arquivos e metadados organizados por data  
  Automatic file and metadata generation organized by date

- Logger bilíngue e estrutura de testes com Pytest  
  Bilingual logger and test structure using Pytest

- Modular, escalável e fácil de adaptar a novos contextos  
  Modular, scalable, and easy to adapt for new contexts

---

## 📁 Estrutura do Projeto  / 📁 Project Structure

```bash
PIPELINE_CRAFT/
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
│   │   └── web_scraping_template.py
│   │
│   └── polars_templates/    # Templates com Polars / Templates using Polars
│       ├── api_template.py
│       ├── csv_template.py
│       ├── databases_template.py
│       ├── sharepoint_xls_template.py
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
│   └── logger.py             # Logger bilíngue / Bilingual logger
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
git clone https://github.com/mpraes/pipeline_craft.git
cd seu-repositorio
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
```bash	
touch .env
```	
5️⃣ (OPCIONAL/OPTIONAL) Execute os testes automáticos / Run automatic tests
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

- [x] Templates de ingestão com Pandas e Polars
- [x] Logger bilíngue para rastreamento dos processos
- [x] Estrutura completa de testes unitários com Pytest
- [x] Makefile configurado para rodar testes e scripts facilmente
- [x] Templates de limpeza de dados (Pandas)
- [x] Templates de limpeza de dados (Polars e DuckDB - versão inicial)
- [x] Estrutura modularizada e profissional de ingestão
- [ ] Templates de transformação avançada (pós-pré-processamento para silver)
- [ ] Adicionar integração contínua (CI/CD) com GitHub Actions
- [ ] Criar imagens Docker para ambientes de execução padronizados
- [ ] Melhorar exemplos de `.env` para múltiplos bancos de dados
- [ ] Expandir o suporte para autenticação OAuth2 em APIs


## 🛠️ Próximos Passos / Next Steps

- [ ] Finalizar e aprimorar os templates de limpeza de dados para Silver Layer
- [ ] Criar templates de transformação avançada e derivação de métricas
- [ ] Adicionar exemplos práticos para consumo via DuckDB e Parquet
- [ ] Implementar GitHub Actions para rodar testes automaticamente em cada push
- [ ] Criar imagens Docker padronizadas para ambientes de desenvolvimento/teste
- [ ] Adicionar documentação de exemplos de pipelines completos (bronze → silver → gold)
- [ ] Melhorar integração para ingestão de APIs autenticadas (OAuth2, Tokens)
- [ ] Adicionar versionamento de metadados e histórico de ingestões
- [ ] Melhorar suporte a falhas com tratamento mais robusto de erros


Contribuições são bem-vindas!
Contributions are welcome!

Solicite adição de contribuidor, e com isso crie uma branch e abra um pull request com sugestões, melhorias ou novos templates. Pode também abrir issues ou até entrar em contato comigo com sugestões.
Feel free to request contributor access, create a branch, and open a pull request with suggestions, improvements, or new templates. You can also open issues or contact me directly with suggestions.

Distribuído sob a licença MIT.
Distributed under the MIT license.
Use livre para fins comerciais ou educacionais.
Free to use for commercial or educational purposes.


