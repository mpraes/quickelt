# Setup de Infraestrutura

Quando eu já tenho os requisitos funcionais definidos 

 - fontes de dados;
 - regras de negócio para transformação;
 - separação de camadas para limpeza e apliação das regras de negócio.

O próximo passo mais provável é a criação de uma infraestrutura para aplicar o projeto Quickelt

O Quickelt se baseia nas premissas:

 - Não haverá transformação na ingestão, mas somente quando os dados da fonte estiverem na "landing_zone" ou "bronze";
 - É recomendado uso de um Datalake, devido a flexíbilidade para transformações, configuração de pastas e segurança prontas e menor custo de armazenamento na camada intermediária devido os arquivos parquet/avro/orc;
 - O código será aplicado, monitorado e em funcionamento dentro de uma VM, porém o Lake pode estar fora (S3, ADLS) em alguma cloud, logo precisa haver configuração pronta de conexão.

O setup de infra será todo interativo via shell (terminal), logo o usuário pode optar por Bash (linux) ou Powershell (Windows).

A primeira parte condidional do fluxo de decisões do usuário na infra será:

- Se os scripts de ETL serão na máquina que ele está ou se precisa criar uma VM para "depositar os scripts", ou outra opção como rodar em Azure Functions ou AWS Lambda ou até AWS Glue

Na segunda parte Dependendo da cloud escolhida, precisa de interação com o CLI da cloud:

- Verificar scripts bash e powershell que possam fazer a instalação caso não haja no terminal

Na terceira parte, o usuário pode informar se já tem pronto um Lake ou Warehouse database (ou um Nosql) para as camadas "prata" e "ouro", lembrando que as transformações só no processo cujos dados já estão dentro do "Lakehouse"

- Subir o lake (ou warehouse) e configurar pastas via script rápido para ficar pronto para uso

- Subir as instancias relativas necessárias para que o usuário possa conectar e usar (seja VM, seja algum recurso que ele possa interagir e criar/rodar os scripts python)


