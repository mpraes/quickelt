# ✅ Checklist de Boas Práticas do Projeto

Este checklist organiza e orienta a manutenção da qualidade do projeto de Engenharia de Dados.

---

## 📚 Estrutura e Organização

- [x] Separar `data/bronze/` para dados brutos.
- [x] Separar `metadata/` para arquivos de metadados das ingestões.
- [x] Usar `ingestion/pandas_templates/` e `ingestion/polars_templates/` para manter ingestões organizadas por tecnologia.
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
