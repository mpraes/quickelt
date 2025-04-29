# ✅ Checklist de Boas Práticas do Projeto

Este checklist organiza e orienta a manutenção da qualidade do projeto de Engenharia de Dados.

---

## 📚 Estrutura e Organização

- [x] Separar `data/bronze/` para dados brutos.
- [x] Separar `metadata/` para arquivos de metadados das ingestões.
- [x] Criar pastas separadas `ingestion/pandas_templates/` e `ingestion/polars_templates/` para manter ingestões organizadas por tecnologia caso queira usar os dois frameworks.
- [x] Usar `tests/` para todos os testes automatizados.
- [x] Usar `utils/` para ferramentas auxiliares como logger.

---

## 🛠️ Boas Práticas de Desenvolvimento

- [x] Nomear arquivos de dados e metadados com o padrão `{origem}_{formato}_{timestamp}`.
- [x] Sempre gerar um arquivo de metadados (`_metadata.json`) para cada ingestão realizada.
- [x] Usar Logger bilíngue (`utils/logger.py`) para padronizar mensagens de log.
- [x] Carregar variáveis de ambiente a partir do `.env` (usando `python-dotenv`).
- [x] Garantir a criação de diretórios no início dos scripts (bronze, metadata).

---

## 🧪 Boas Práticas de Testes

- [x] Criar testes automáticos (`pytest`) para validar se arquivos de dados e metadados são gerados corretamente.
- [x] Validar a integridade dos arquivos de metadados como JSON válido.
- [x] Garantir ambiente limpo antes de rodar testes (via `conftest.py`).
- [x] Carregar `.env` automaticamente para testes usando fixture de sessão no `conftest.py`.

---

## 📥 Boas Práticas de Instalação e Setup

- [x] Disponibilizar um `requirements.txt` claro e atualizado.
- [x] Fornecer um `README.md` completo com:
  - [x] Propósito do projeto
  - [x] Como instalar
  - [x] Status atual
  - [x] Estrutura de diretórios

---

## 🚀 Futuras Boas Práticas (planejado)

- [ ] Criar templates de **transformação de dados** padronizados.

---

[en]
# ✅ Best Practices Checklist for the Project

This checklist organizes and guides the maintenance of the quality of the Data Engineering project.

---

## 📚 Structure and Organization

- [x] Separate `data/bronze/` for raw data.
- [ ] Separate `metadata/` for ingestion metadata files.
- [ ] Create separate folders `ingestion/pandas_templates/` and `ingestion/polars_templates/` to keep ingestions organized by technology if you want to use both frameworks.
- [ ] Use `tests/` for all automated tests.
- [ ] Use `utils/` for auxiliary tools like logger.
- [ ] Use `transformation/` for data transformation templates.

---

## 🛠️ Development Best Practices

- [x] Name data and metadata files with the pattern `{origin}_{format}_{timestamp}`.
- [ ] Always generate a metadata file (`_metadata.json`) for each ingestion performed.
- [ ] Use Bilingual Logger (`utils/logger.py`) to standardize log messages.
- [ ] Load environment variables from `.env` (using `python-dotenv`).
- [ ] Ensure the creation of directories at the beginning of scripts (bronze, metadata).

---

## 🧪 Testing Best Practices

- [ ] Create automated tests (`pytest`) to validate if data and metadata files are generated correctly.
- [ ] Validate the integrity of metadata files as valid JSON.
- [ ] Ensure a clean environment before running tests (via `conftest.py`).
- [ ] Automatically load `.env` for tests using session fixture in `conftest.py`.

---

## 📥 Installation and Setup Best Practices

- [x] Provide a clear and updated `requirements.txt`.
- [x] Offer a complete `README.md` with:
  - [x] Purpose of the project
  - [x] How to install
  - [x] Current status
  - [x] Directory structure

