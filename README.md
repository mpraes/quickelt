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
4️⃣ Configure variáveis de ambiente / Configure environment variables
```bash	
cp .env.example .env
```	
5️⃣ Execute os testes automáticos / Run automatic tests
```bash	
pytest
```	
---

Visite o [CHECKLIST.md](CHECKLIST.md) para mais informações sobre boas práticas e orientações.  
Visit the [CHECKLIST.md](CHECKLIST.md) for more information on best practices and guidelines.

## 📈 Status Atual / Current Status

- [x] Templates de ingestão com Pandas e Polars
- [x] Logger bilíngue
- [x] Estrutura de testes com Pytest
- [x] Modular e fácil de adaptar
- [ ] Templates de transformação de dados (em andamento)

## 🛠️ Próximos Passos / Next Steps

- [ ] Adicionar templates de transformação de dados
- [ ] Melhorar a documentação
- [ ] Adicionar mais exemplos de uso
- [ ] Adicionar mais testes automatizados
- [ ] Aprimorar a estrutura de diretórios
- [ ] Adicionar mais funcionalidades
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


