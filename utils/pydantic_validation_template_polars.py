"""
pydantic_validation.py
-----------------------

Funções utilitárias para validação de DataFrames utilizando Pydantic.
Utility functions for validating DataFrames using Pydantic.

Dependências / Dependencies:
- pydantic
- polars
"""

import polars as pl
from typing import Type
from pydantic import BaseModel, TypeAdapter

def validate_with_pydantic_batch(
    df: pl.DataFrame,
    model: Type[BaseModel],
    strict: bool = True
) -> pl.DataFrame:
    """
    Valida um DataFrame Polars usando um modelo Pydantic em modo batch.
    Validate a Polars DataFrame using a Pydantic model in batch mode.

    Parâmetros / Parameters:
    - df: pl.DataFrame -> DataFrame de entrada / Input DataFrame
    - model: BaseModel -> Modelo Pydantic para validação / Pydantic Model for validation
    - strict: bool -> Se True, rejeita colunas extras / If True, rejects unexpected columns

    Retorna / Returns:
    - pl.DataFrame validado / validated pl.DataFrame
    """

    expected_columns = set(model.model_fields.keys())
    received_columns = set(df.columns)

    extra_columns = received_columns - expected_columns
    missing_columns = expected_columns - received_columns

    if strict:
        if extra_columns:
            raise ValueError(
                f"Colunas inesperadas encontradas: {extra_columns} / Unexpected columns found: {extra_columns}"
            )

    if missing_columns:
        raise ValueError(
            f"Colunas obrigatórias ausentes: {missing_columns} / Required columns missing: {missing_columns}"
        )

    # Validar dados em batch
    adapter = TypeAdapter(list[model])

    try:
        validated_data = adapter.validate_python(df.to_dicts())
    except Exception as e:
        raise ValueError(
            f"Erro de validação Pydantic: {str(e)} / Pydantic validation error: {str(e)}"
        )

    # Retornar novo DataFrame validado
    validated_df = pl.DataFrame([item.model_dump() for item in validated_data])

    return validated_df
