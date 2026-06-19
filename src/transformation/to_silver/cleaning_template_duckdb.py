"""
Data Cleaning Template Guide
-----------------------------

This template provides the most possible comprehensive framework for data cleaning using DuckDB.
Follow these guidelines when using this template.
This template is designed for reading only Parquet files.

[PT-BR]
Guia do Template de Limpeza de Dados
------------------------------------

Este template fornece uma estrutura mais abrangente possível para limpeza de dados usando DuckDB.
Siga estas diretrizes ao usar este template.
Este template foi projetado para leitura apenas de arquivos Parquet.
"""

"""
1. BEFORE USING THIS TEMPLATE / ANTES DE USAR ESTE TEMPLATE:
   - Make a backup of your original data / Faça um backup dos seus dados originais
   - Understand your data structure and requirements / Compreenda a estrutura e requisitos dos seus dados
   - Document your initial data quality issues / Documente os problemas iniciais de qualidade dos dados
   - Define acceptable values and ranges for your variables / Defina valores e intervalos aceitáveis para suas variáveis

2. CUSTOMIZATION POINTS / PONTOS DE PERSONALIZAÇÃO:
   - Missing values: Adjust the filling strategy based on your data context / Valores ausentes: Ajuste a estratégia de preenchimento conforme o contexto
   - Outliers: Modify the IQR multiplier (1.5) based on your needs / Outliers: Modifique o multiplicador IQR (1.5) conforme necessário
   - Text cleaning: Add specific text cleaning rules for your case / Limpeza de texto: Adicione regras específicas para seu caso
   - Categories: Adjust the rare category threshold (currently 1%) / Categorias: Ajuste o limite para categorias raras (atualmente 1%)
   - Sparse columns: Modify the missing value threshold (currently 70%) / Colunas esparsas: Modifique o limite de valores nulos (atualmente 70%)

3. BEST PRACTICES / BOAS PRÁTICAS:
   - Always validate results after each cleaning step / Sempre valide os resultados após cada etapa
   - Document any modifications made to the original template / Documente quaisquer modificações
   - Keep track of rows/values removed or modified / Mantenha registro das alterações
   - Consider the impact of each cleaning operation on your analysis / Considere o impacto de cada limpeza

4. USAGE STEPS / ETAPAS DE USO:
   1. Import this template / Importe este template
   2. Load your data / Carregue seus dados
   3. Configure cleaning parameters / Configure os parâmetros de limpeza
   4. Run the cleaning process / Execute o processo de limpeza
   5. Validate results / Valide os resultados
   6. Export cleaned data / Exporte os dados limpos

5. VALIDATION CHECKLIST / CHECKLIST DE VALIDAÇÃO:
   - Check data types are correct / Verifique se os tipos de dados estão corretos
   - Verify missing values are handled appropriately / Verifique se valores ausentes foram tratados
   - Confirm no unexpected nulls were introduced / Confirme que não surgiram nulos inesperados
   - Validate categorical variables are standardized / Valide as variáveis categóricas
   - Review outlier treatment results / Revise o tratamento de outliers
"""

import duckdb
import re

def standardize_column_name(col_name: str) -> str:
    """
    Standardizes a single column name by applying lowercase, replacing non-alphanumeric characters with underscores,
    and cleaning redundant underscores.
    
    [PT-BR]
    Padroniza o nome de uma coluna aplicando letras minúsculas, substituindo caracteres não alfanuméricos por underscores
    e limpando underscores redundantes.
    """
    col = col_name.lower()
    col = re.sub(r'[^a-z0-9]', '_', col)
    col = col.strip('_')
    col = re.sub(r'_+', '_', col)
    return col

def build_select_clause(columns: list) -> str:
    """
    Builds a dynamic SELECT clause mapping original column names to standardized names.
    
    [PT-BR]
    Constrói uma cláusula SELECT dinâmica mapeando os nomes originais para nomes padronizados.
    """
    select_expressions = []
    for col in columns:
        standardized_col = standardize_column_name(col)
        if col != standardized_col:
            select_expressions.append(f'"{col}" AS {standardized_col}')
        else:
            select_expressions.append(f'"{col}"')
    return ", ".join(select_expressions)

def read_and_standardize_parquet(parquet_path: str, con: duckdb.DuckDBPyConnection = None) -> duckdb.DuckDBPyRelation:
    """
    Reads a Parquet file and standardizes column names dynamically.

    [PT-BR]
    Lê um arquivo Parquet e padroniza os nomes das colunas dinamicamente.
    """
    if con is None:
        con = duckdb.connect()  # Open connection if not provided / Abre conexão se não fornecida

    columns = [col[0] for col in con.execute(f"PRAGMA table_info('{parquet_path}')").fetchall()]  # Reads columns metadata / Lê metadados das colunas
    select_clause = build_select_clause(columns)
    query = f"SELECT {select_clause} FROM '{parquet_path}'"

    return con.execute(query)

def handle_missing_values_duckdb(columns_defaults: dict) -> str:
    """
    Generates CASE expressions to replace NULL values with default values.
    
    [PT-BR]
    Gera expressões CASE para substituir valores NULL por valores padrão.
    
    Example / Exemplo:
    columns_defaults = {'coluna1': '0', 'coluna2': "'desconhecido'"}
    """
    expressions = []
    for col, default in columns_defaults.items():
        expressions.append(f"COALESCE({col}, {default}) AS {col}")
    return ", ".join(expressions)

def fix_data_types_duckdb(columns_types: dict) -> str:
    """
    Generates CAST expressions to enforce specific data types.
    
    [PT-BR]
    Gera expressões CAST para forçar tipos de dados específicos.
    
    Example / Exemplo:
    columns_types = {'coluna1': 'INTEGER', 'coluna2': 'VARCHAR'}
    """
    expressions = []
    for col, dtype in columns_types.items():
        expressions.append(f"CAST({col} AS {dtype}) AS {col}")
    return ", ".join(expressions)

def clean_text_data_duckdb(columns: list) -> str:
    """
    Applies TRIM and LOWER functions to clean text columns.
    
    [PT-BR]
    Aplica funções TRIM e LOWER para limpar colunas de texto.
    """
    expressions = []
    for col in columns:
        expressions.append(f"LOWER(TRIM({col})) AS {col}")
    return ", ".join(expressions)

def standardize_categories_duckdb(column: str, mappings: dict) -> str:
    """
    Generates a CASE WHEN expression to standardize categorical values.
    
    [PT-BR]
    Gera uma expressão CASE WHEN para padronizar valores categóricos.
    
    Example / Exemplo:
    mappings = {'velho': 'antigo', 'novo': 'recente'}
    """
    cases = [f"WHEN {column} = '{old}' THEN '{new}'" for old, new in mappings.items()]
    case_statement = f"CASE {' '.join(cases)} ELSE {column} END AS {column}"
    return case_statement

def validate_data_duckdb(column_checks: dict) -> list:
    """
    Generates validation queries for data quality checks (e.g., range validation).
    
    [PT-BR]
    Gera consultas de validação para checagem de qualidade dos dados (ex.: validação de intervalo).
    
    Example / Exemplo:
    column_checks = {'idade': {'min': 0, 'max': 120}}
    """
    queries = []
    for col, checks in column_checks.items():
        if 'min' in checks:
            queries.append(f"SELECT COUNT(*) FROM tabela WHERE {col} < {checks['min']}")
        if 'max' in checks:
            queries.append(f"SELECT COUNT(*) FROM tabela WHERE {col} > {checks['max']}")
    return queries

def handle_dates_duckdb(columns: dict) -> str:
    """
    Generates date handling expressions for standardization and validation.
    
    [PT-BR]
    Gera expressões para tratamento e validação de datas.
    
    Args:
        columns (dict): Dictionary with column names and their date formats
                       Dicionário com nomes das colunas e seus formatos de data
                       Example: {'data_col': {'format': 'YYYY-MM-DD', 'timezone': 'UTC'}}
    
    Returns:
        str: SQL expressions for date handling
             Expressões SQL para tratamento de datas
    """
    expressions = []
    for col, config in columns.items():
        format_str = config.get('format', 'YYYY-MM-DD')
        timezone = config.get('timezone', 'UTC')
        expressions.append(f"TRY_STRPTIME({col}, '{format_str}') AT TIME ZONE '{timezone}' AS {col}")
    return ", ".join(expressions)

def handle_currency_duckdb(columns: dict) -> str:
    """
    Generates currency handling expressions for standardization.
    
    [PT-BR]
    Gera expressões para padronização de valores monetários.
    
    Args:
        columns (dict): Dictionary with column names and their currency configs
                       Dicionário com nomes das colunas e suas configurações de moeda
                       Example: {'valor': {'decimal_places': 2, 'currency': 'BRL'}}
    
    Returns:
        str: SQL expressions for currency handling
             Expressões SQL para tratamento de moeda
    """
    expressions = []
    for col, config in columns.items():
        decimal_places = config.get('decimal_places', 2)
        currency = config.get('currency', 'BRL')
        expressions.append(f"ROUND(CAST({col} AS DECIMAL(18,{decimal_places})), {decimal_places}) AS {col}")
    return ", ".join(expressions)

def handle_duplicates_duckdb(columns: list, strategy: str = 'keep_first') -> str:
    """
    Generates expressions for duplicate handling.
    
    [PT-BR]
    Gera expressões para tratamento de duplicatas.
    
    Args:
        columns (list): List of columns to check for duplicates
                       Lista de colunas para verificar duplicatas
        strategy (str): Strategy for handling duplicates ('keep_first', 'keep_last', 'keep_none')
                       Estratégia para tratar duplicatas
    
    Returns:
        str: SQL expressions for duplicate handling
             Expressões SQL para tratamento de duplicatas
    """
    if strategy == 'keep_first':
        return f"SELECT DISTINCT ON ({', '.join(columns)}) * FROM tabela"
    elif strategy == 'keep_last':
        return f"SELECT DISTINCT ON ({', '.join(columns)}) * FROM tabela ORDER BY {', '.join(columns)}, rowid DESC"
    else:
        return f"SELECT * FROM tabela WHERE ({', '.join(columns)}) NOT IN (SELECT {', '.join(columns)} FROM tabela GROUP BY {', '.join(columns)} HAVING COUNT(*) > 1)"

def enrich_data_duckdb(join_config: dict) -> str:
    """
    Generates expressions for data enrichment through joins.
    
    [PT-BR]
    Gera expressões para enriquecimento de dados através de joins.
    
    Args:
        join_config (dict): Configuration for joins
                           Configuração para joins
                           Example: {'table': 'reference', 'type': 'LEFT', 'on': 'id = ref.id'}
    
    Returns:
        str: SQL expressions for data enrichment
             Expressões SQL para enriquecimento de dados
    """
    return f"SELECT t.*, r.* FROM tabela t {join_config['type']} JOIN {join_config['table']} r ON {join_config['on']}"

def validate_integrity_duckdb(checks: dict) -> list:
    """
    Generates validation queries for data integrity checks.
    
    [PT-BR]
    Gera consultas de validação para checagem de integridade dos dados.
    
    Args:
        checks (dict): Dictionary with integrity check configurations
                      Dicionário com configurações de checagem de integridade
                      Example: {'foreign_key': {'column': 'id', 'reference': 'ref_table.id'}}
    
    Returns:
        list: List of validation queries
              Lista de consultas de validação
    """
    queries = []
    for check_type, config in checks.items():
        if check_type == 'foreign_key':
            queries.append(f"SELECT COUNT(*) FROM tabela t LEFT JOIN {config['reference'].split('.')[0]} r ON t.{config['column']} = r.{config['reference'].split('.')[1]} WHERE r.{config['reference'].split('.')[1]} IS NULL")
    return queries

# 🚀 EXAMPLE OF USAGE / EXEMPLO DE USO

if __name__ == "__main__":
    # Initialize connection / Inicializa conexão
    con = duckdb.connect()
    
    # Example data path / Caminho do arquivo de exemplo
    parquet_file = 'data/example.parquet'
    
    # Load and standardize columns / Carrega e padroniza colunas
    relation = read_and_standardize_parquet(parquet_file, con)
    
    # Example configurations / Configurações de exemplo
    
    # 1. Date handling / Tratamento de datas
    date_config = {
        'data_compra': {'format': 'DD/MM/YYYY', 'timezone': 'America/Sao_Paulo'},
        'data_entrega': {'format': 'YYYY-MM-DD', 'timezone': 'UTC'}
    }
    
    # 2. Currency handling / Tratamento de moeda
    currency_config = {
        'valor_total': {'decimal_places': 2, 'currency': 'BRL'},
        'valor_frete': {'decimal_places': 2, 'currency': 'BRL'}
    }
    
    # 3. Duplicate handling / Tratamento de duplicatas
    duplicate_columns = ['id_pedido', 'data_compra']
    
    # 4. Data enrichment / Enriquecimento de dados
    join_config = {
        'table': 'clientes',
        'type': 'LEFT',
        'on': 't.id_cliente = c.id'
    }
    
    # 5. Integrity validation / Validação de integridade
    integrity_checks = {
        'foreign_key': {
            'column': 'id_cliente',
            'reference': 'clientes.id'
        }
    }
    
    # Build and execute cleaning pipeline / Constrói e executa pipeline de limpeza
    query = f"""
    WITH standardized_dates AS (
        SELECT {handle_dates_duckdb(date_config)}
        FROM {relation}
    ),
    standardized_currency AS (
        SELECT {handle_currency_duckdb(currency_config)}
        FROM standardized_dates
    ),
    deduplicated AS (
        {handle_duplicates_duckdb(duplicate_columns, 'keep_first')}
        FROM standardized_currency
    ),
    enriched AS (
        {enrich_data_duckdb(join_config)}
        FROM deduplicated t
    )
    SELECT * FROM enriched
    """
    
    # Execute final query / Executa query final
    result = con.execute(query)
    
    # Validate integrity / Valida integridade
    validation_queries = validate_integrity_duckdb(integrity_checks)
    for query in validation_queries:
        invalid_count = con.execute(query).fetchone()[0]
        if invalid_count > 0:
            print(f"Warning: Found {invalid_count} records with integrity issues")
            print(f"Aviso: Encontrados {invalid_count} registros com problemas de integridade")
    
    # Export results / Exporta resultados
    result.to_parquet('data/cleaned_data.parquet')
    
    print("Pipeline executed successfully!")
    print("Pipeline executado com sucesso!")
