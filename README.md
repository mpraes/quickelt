# Data Engineering Project Template

## 📌 Propósito do Projeto

Este projeto foi criado para ajudar **engenheiros de dados** a iniciarem rapidamente seus próprios projetos profissionais.

✅ Aqui você encontrará **templates prontos** para:
- Ingestão de dados (usando Pandas ou Polars)
- Padrões de qualidade como nomeação automática de arquivos, geração de metadados e logger bilíngue.
- Ambiente preparado para testes automatizados (`pytest`) com configuração automática de ambiente (`conftest.py`).

O objetivo é permitir que você:
- Economize tempo no setup inicial dos seus projetos de dados.
- Comece seus pipelines já com boas práticas de Engenharia de Dados.
- Tenha uma estrutura modular e fácil de escalar.

---

## 📥 Como baixar e configurar no seu computador

**Passo 1**: Clone o repositório

```bash
git clone https://github.com/seu-usuario/seu-repositorio.git
cd seu-repositorio
```

**Passo 2**: Crie e ative um ambiente virtual

# Com venv (padrão Python)
python -m venv .venv
source .venv/bin/activate  # Linux/macOS
.venv\Scripts\activate     # Windows

**Passo 3**: Instale as dependências

**Passo 4**: Configure o arquivo `.env` com suas variáveis de ambiente

**Passo 5** (opcional) Execute os testes para validar o ambiente

```bash
pytest tests/
```

- [✅ Confira o nosso Checklist de Boas Práticas](CHECKLIST.md)

## ⚙️ Estrutura do Projeto

.
├── data/
│   └── bronze/          # Dados ingeridos
├── ingestion/
│   ├── pandas_templates/
│   │   ├── api_template.py
│   │   ├── csv_template.py
│   │   └── databases_template.py
│   └── polars_templates/
│       ├── api_template.py
│       ├── csv_template.py
│       └── databases_template.py
├── metadata/            # Metadados das ingestões
├── tests/
│   ├── test_ingestion_pandas.py
│   ├── test_ingestion_polars.py
│   └── conftest.py
├── utils/
│   └── logger.py        # Logger bilíngue
├── .env                 # Arquivo de variáveis de ambiente
├── .gitignore           # Arquivos e diretórios ignorados pelo Git
├── README.md            # Este arquivo
└── requirements.txt     # Dependências Python

## 📈 Status Atual do Projeto

### ✅ Finalizada a primeira etapa de Ingestão de Dados:

- Templates prontos para ingestão de API, CSV, Banco de Dados, SharePoint e Web Scraping.
- Disponíveis em duas versões: Pandas (focado em simplicidade) e Polars (focado em performance).
- Geração automática de arquivos de dados e metadados organizados por data.
- Estrutura de testes automáticos completa usando pytest.

### 🚧 Próximos Passos:

- Iniciar a etapa de Transformação de Dados (próxima camada).
- Criar templates de transformação padronizada.
- Melhorar continuamente o projeto com padrões de Data Engineering modernos.

## 🤝 Contribuições
Contribuições são bem-vindas!

## 📄 Licença
Este projeto é de uso livre sob a licença MIT License.

