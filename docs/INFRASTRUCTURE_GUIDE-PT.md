# Quickelt: Guia de Arquitetura de Infraestrutura Automatizada
## O que acontece nos bastidores quando você roda o CLI?

Este documento serve como um guia visual e técnico para que o Engenheiro de Dados compreenda exatamente quais recursos são tocados, validados e criados na Nuvem ao executar o assistente interativo do **Quickelt**.

---

## 🗺️ Fluxo de Execução e Tomada de Decisão

Quando você inicia o script `setup.py` no seu terminal, a ferramenta executa um fluxo linear dividido em 4 etapas críticas:

```
[1. Validação] ──> [2. Criação do Lake] ──> [3. Setup de Computação] ──> [4. Data Warehouse (opc)] ──> [5. Entrega (.env)]
```

### Etapa 1: Validação de Contexto e Autenticação
Antes de solicitar qualquer dado, o CLI garante que o ambiente local é seguro e possui as dependências necessárias para interagir com o provedor escolhido.

* **Identificação do SO:** O script detecta se você está em Linux, macOS ou Windows.
* **Checagem de Credenciais:** * **AWS:** Executa implicitamente `aws sts get-caller-identity`. Se falhar, avisa que o token expirou ou que o `aws configure` precisa ser executado.
    * **Azure:** Executa `az account show`. Se o terminal não estiver logado, instrui o uso imediato do `az login`.

---

### Etapa 2: Provisionamento do Storage (Data Lakehouse)
A premissa do Quickelt é que nenhuma transformação ocorre antes do dado pousar na camada inicial. O CLI automatiza a criação do seu repositório de dados.

#### Se você optar por criar um novo Lake:
O script invoca as ferramentas nativas de infraestrutura para provisionar um storage de baixo custo e alta performance, estruturando imediatamente o ecossistema de pastas.

* **Na AWS (S3):**
    1. Executa: `aws s3api create-bucket --bucket <nome-do-lake> --region <regiao>`
    2. Bloqueia o acesso público para garantir conformidade de segurança (LGPD/GDPR).
    3. Cria delimitadores de objetos (pastas virtuais) simulando o padrão Lakehouse:
        * `<nome-do-lake>/bronze/` (Dados brutos / Landing Zone)
        * `<nome-do-lake>/silver/` (Dados limpos e enriquecidos em Parquet/Avro)
        * `<nome-do-lake>/gold/` (Camada analítica / Consumo)

* **Na Azure (ADLS Gen2 / Blob Storage):**
    1. Cria um Grupo de Recursos (caso não exista): `az group create`
    2. Provisiona a Conta de Armazenamento: `az storage account create` habilitando o *Hierarchical Namespace* (essencial para performance de Big Data).
    3. Cria o Container principal e injeta a estrutura de diretórios `/bronze`, `/silver` e `/gold`.

---

### Etapa 3: Configuração da Camada de Computação
Dependendo de onde você escolheu que o motor de processamento do Quickelt deve rodar, o comportamento da infraestrutura muda de maneira transparente:

1.  **Máquina Local:** Nenhuma infraestrutura de computação é criada na nuvem. O CLI assume que você rodará os scripts Python localmente ou em containers Docker próprios.
2.  **VM Dedicada (Servidor de Processamento):**
    * **AWS (EC2):** Provisiona uma instância (padrão `t3.medium`) rodando Ubuntu Server via `aws ec2 run-instances`. Dispara um script de *UserData* na inicialização da máquina que atualiza os pacotes e instala o `python3-pip` e o `git` automaticamente.
    * **Azure (VM):** Cria uma máquina virtual Linux via `az vm create` e aplica uma extensão de script customizada para garantir que o ambiente Python esteja pronto para uso imediato.
3.  **Serverless / PaaS (Escalabilidade Automática):**
    * Prepara os mapeamentos de ambiente e permissões (IAM) necessários para que serviços como AWS Lambda/Glue ou Azure Functions consigam ler a camada `bronze` e gravar na `silver/gold` sem expor chaves públicas.

---

### Etapa 4: Provisionamento do Data Warehouse (Opcional)
Se você selecionou um banco de dados Gold externo, o Quickelt oferece duas estratégias:

1. **PostgreSQL Local dentro da VM** — Instala o PostgreSQL dentro da EC2/VM provisionada, cria um usuário `quickelt` com senha aleatória criptograficamente segura (`secrets.token_urlsafe`) e configura o banco `quickelt_db` automaticamente.
2. **Serviço Gerenciado na Nuvem** — Provisiona um cluster PostgreSQL gerenciado:
    * **AWS:** Cluster Aurora PostgreSQL via `aws rds create-db-cluster` com instância primária.
    * **Azure:** Azure DB for PostgreSQL Flexible Server via `az postgres flexible-server create`.
    * Se você optar por conectar a um cluster existente, o assistente coleta host, porta, usuário e senha interativamente.

Todas as senhas geradas utilizam `secrets.token_urlsafe(32)` — nunca padrões hardcoded. Quando um cluster gerenciado já existe, o assistente o reutiliza (com no máximo 3 tentativas de retry para conflitos de nome).

---

### Etapa 5: Persistência de Estado (.env)
A última ação do CLI é puramente local. Todas as decisões tomadas por você, caminhos criados e IDs de instâncias geradas são consolidados em um arquivo `.env` na raiz do seu projeto.

**Importante:** O escritor do `.env` realiza um **merge** — atualiza apenas as chaves gerenciadas pelo assistente, preservando quaisquer variáveis customizadas que você tenha adicionado manualmente. Chaves existentes do assistente são atualizadas in-place, e novas chaves são adicionadas. Comentários e entradas customizadas nunca são removidos.

Quando a camada Gold utiliza um banco de dados externo, as permissões do arquivo `.env` são restritas a `0o600` (leitura/escrita apenas pelo dono) para proteger credenciais.

Isso significa que a sua infraestrutura passa a ser documentada por variáveis. Se você precisar destruir ou recriar o ambiente, o Quickelt usará esse arquivo como mapa estático.

---

## 🔒 Segurança e Melhores Práticas Aplicadas

* **Princípio do Menor Privilégio:** Todas as criações utilizam o contexto do usuário previamente logado no CLI da nuvem, respeitando as políticas de IAM existentes.
* **Sem Senhas Hardcoded:** Todas as senhas são geradas em runtime usando `secrets.token_urlsafe()` — a codebase nunca contém credenciais padrão.
* **Proteção do arquivo .env:** Quando credenciais de banco estão presentes, o `.env` é automaticamente configurado com permissões `0o600` (apenas dono).
* **Escrita Merge-Only:** O escritor do `.env` preserva entradas e comentários customizados do usuário, atualizando apenas chaves gerenciadas pelo assistente.
* **Proteção contra Retry Infinito:** O tratamento de conflitos de nome é limitado a 3 tentativas para evitar recursão infinita.
* **Custo Otimizado:** Os storages são configurados por padrão sem versionamento excessivo ou redundâncias globais desnecessárias para a fase inicial de desenvolvimento, poupando custos operacionais na camada intermediária.