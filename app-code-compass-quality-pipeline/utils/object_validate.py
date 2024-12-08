from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, ArrayType
)

# Esquema para Google Play
def google_play_schema():
    return StructType([
        StructField("id", StringType(), True),
        StructField("app", StringType(), True),
        StructField("rating", DoubleType(), True),
        StructField("iso_date", StringType(), True),
        StructField("title", StringType(), True),
        StructField("snippet", StringType(), True),
        StructField("historical_data", ArrayType(
            ArrayType(StructType([
                StructField("title", StringType(), True),
                StructField("snippet", StringType(), True),
                StructField("app", StringType(), True),
                StructField("rating", StringType(), True),
                StructField("iso_date", StringType(), True)
            ]))
        ), False)
    ])

# Esquema para MongoDB
def mongodb_schema():
    return StructType([
        StructField("id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("cpf", StringType(), True),
        StructField("app", StringType(), True),
        StructField("rating", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("comment", StringType(), True),
        StructField("app_version", StringType(), True),
        StructField("os_version", StringType(), True),
        StructField("os", StringType(), True),
        StructField("historical_data", ArrayType(
            ArrayType(StructType([
                StructField("customer_id", StringType(), True),
                StructField("cpf", StringType(), True),
                StructField("app", StringType(), True),
                StructField("comment", StringType(), True),
                StructField("rating", StringType(), True),
                StructField("timestamp", StringType(), True),
                StructField("app_version", StringType(), True),
                StructField("os_version", StringType(), True),
                StructField("os", StringType(), True)
            ]))
        ), False)
    ])

# Esquema para Play Store
def play_store_schema():
    return StructType([
        StructField("id", StringType(), True),
        StructField("app", StringType(), True),
        StructField("rating", DoubleType(), True),
        StructField("iso_date", StringType(), True),
        StructField("title", StringType(), True),
        StructField("snippet", StringType(), True),
        StructField("historical_data", ArrayType(
            ArrayType(StructType([
                StructField("title", StringType(), True),
                StructField("snippet", StringType(), True),
                StructField("app", StringType(), True),
                StructField("rating", StringType(), True),
                StructField("iso_date", StringType(), True)
            ]))
        ), False)
    ])

def get_column_patterns() -> dict:
    """
    Retorna os padrões de validação para cada coluna de um DataFrame.

    :return: Dicionário com os padrões de validação para cada coluna.
    """
    return {
        "column_name_1": {
            "type": "string",  # Espera tipo string
            "regex": "^[a-zA-Z0-9]*$",  # Alfanumérico
            "not_null": True  # Não permite valores nulos
        },
        "column_name_2": {
            "type": "int",  # Espera tipo inteiro
            "min": 0,  # Valor mínimo 0
            "max": 100,  # Valor máximo 100
            "not_null": False  # Permite valores nulos
        }
    }