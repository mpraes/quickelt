# 🚀 The Pipeline Craft  
**Template de Setup para Desenvolvedores de Engenharia de Dados**  
**Setup Template for Data Engineering Developers**

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
│   └── test_ingestion_polars.py
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

Visite o [CHECKLIST.md](CHECKLIST.md) para mais informações sobre boas práticas e orientações.  
Visit the [CHECKLIST.md](CHECKLIST.md) for more information on best practices and guidelines.

## 📈 Status Atual / Current Status

- [x] Templates de ingestão com Pandas e Polars / Ingestion templates with Pandas and Polars
- [x] Logger bilíngue / Bilingual logger
- [x] Estrutura de testes com Pytest / Test structure with Pytest
- [x] Modular e fácil de adaptar / Modular and easy to adapt
- [x] Template de limpeza de dados (Pandas) / Data cleaning template (Pandas)
- [ ] Templates de limpeza (Polars/DuckDB) / Cleaning templates (Polars/DuckDB)
- [ ] Templates de transformação avançada / Advanced transformation templates

## 🛠️ Próximos Passos / Next Steps

- [ ] Completar templates de limpeza (Polars/DuckDB)
- [ ] Adicionar templates de transformação avançada
- [ ] Melhorar documentação e exemplos
- [ ] Expandir suite de testes
- [ ] Adicionar CI/CD
- [ ] Configuração de Docker

Contribuições são bem-vindas!
Contributions are welcome!

Solicite adição de contribuidor, e com isso crie uma branch e abra um pull request com sugestões, melhorias ou novos templates. Pode também abrir issues ou até entrar em contato comigo com sugestões.
Feel free to request contributor access, create a branch, and open a pull request with suggestions, improvements, or new templates. You can also open issues or contact me directly with suggestions.

Distribuído sob a licença MIT.
Distributed under the MIT license.
Use livre para fins comerciais ou educacionais.
Free to use for commercial or educational purposes.


