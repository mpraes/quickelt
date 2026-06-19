# 📥 Considerações Iniciais para Sistemas de Ingestão

Extraído do livro "Fundamentals of Data Engineering" de Joe Reis (O'Reilly, 2022).
[Link para o livro][book-link]

1. Qual é o caso de uso para os dados que estou coletando? Exemplos:
   - Sensores em Maquinário Industrial de uma fábrica para Detecção de falhas iminentes, ou Relatório semanal de eficiência energética
   - Dados do Sitema SAP da tabela de Estoque para relatórios de movimentação  mensal dos produtos acabados no Armazém de uma empresa.
   - Essas perguntas influenciam nas decisões a tomar:
     - Latência
     - Frequência
     - Volume
     - Transformação durante a ingestão
     - Formato
     - Ferramentas e Tecnologias
     - Custos e Infra
Obs: Este projeto é melhor aplicado em casos Open Source, ou seja, dentro de uma Virtual Machine ou Conteiner..

2. É possível reutilizar esses dados e evitar a ingestão de várias versões do mesmo conjunto de dados?
3. Para onde estes dados estão indo?
4. Com qual frequência estes dados devem ser atualizados da fonte?
5. Qual o volume esperado de dados?
6. Em que formato estão os dados? Os sistemas de armazenamento e transformação downstream conseguem aceitar esse formato?
   - Por que é importante?
     - Compatibilidade técnica
     - Eficiência de Armazenamento (Espaço, velocidade de leitura)
     - Facilidade de Processamento e Transformação
     - Evolução de Esquema
     - Qualidade e Parsing
7. Os dados de origem estão em boas condições para uso downstream imediato? Ou seja, os dados tem boa qualidade? Qual pós-processamento é necessário para disponibilizá-los? Quais são os riscos de qualidade dos dados?
    - Por que é importante?
      - Confiabilidade
      - Tomada de Decisão
      - Eficiência Operacional
      - Custo de Correção
    - Dimensões Comuns da qualidade:
      - Precisão
      - Completude
      - Consistência
      - Pontualidade
      - Unicidade
      - Validade
8. Em caso de Streaming, os dados precisam de processamento durante a ingestão downstream?

Essencial considerar esses fatores ao projetar arquitetura de ingestão.

# Main Considerations for Ingestion Systems

Extracted from the book "Fundamentals of Data Engineering" by Joe Reis (O'Reilly, 2022).
[Link para o livro][book-link]

1. What is the use case for the data I am collecting? Examples:
   - Industrial machinery sensors in a factory for early fault detection, or Weekly energy efficiency report
   - SAP System data from the Stock table for monthly reports on finished goods movement in a company's warehouse.
   - These questions influence the decisions to be made:
     - Latency
     - Frequency
     - Volume
     - Transformation during ingestion
     - Format
     - Tools and Technologies
     - Costs and Infrastructure
Obs: This project is better applied in Open Source cases, i.e., within a Virtual Machine or Container..

2. Is it possible to reuse these data and avoid ingesting multiple versions of the same dataset?
3. Where are these data going?
4. How often should these data be updated from the source?
5. What is the expected volume of data?
6. In what format are the data? Do the storage and transformation systems downstream accept this format?
    - Why is it important?
      - Technical Compatibility
      - Storage Efficiency (Space, Read Speed)
      - Ease of Processing and Transformation
      - Schema Evolution
      - Quality and Parsing
7. Are the source data in good condition for immediate downstream use? That is, do the data have good quality? What post-processing is necessary to make them available? What are the quality risks of the data?
    - Why is it important?
      - Reliability
      - Decision Making
      - Operational Efficiency
      - Cost of Correction
    - Common Dimensions of Quality:
      - Precision
      - Completeness
      - Consistency
      - Timeliness
      - Uniqueness
      - Validity
8. In case of Streaming, do the data need processing during downstream ingestion?

[book-link]: https://www.oreilly.com/library/view/fundamentals-of-data/9781098108298/

