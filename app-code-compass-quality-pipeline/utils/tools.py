import os, json, logging, sys, requests, unicodedata, pymongo, pyspark.sql.functions as F
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    regexp_extract, max as spark_max, col, when, input_file_name, lit, date_format, to_date
)
from pyspark.sql.types import (
    MapType, StringType, ArrayType, BinaryType, StructType, StructField, DoubleType, LongType
)
from datetime import datetime
from pathlib import Path
from urllib.parse import quote_plus
from unidecode import unidecode
from tools.object_validate import google_play_schema, mongodb_schema, play_store_schema, get_column_patterns

###########################################################################################################
# Validação leitura da origem e carga
###########################################################################################################
def read_parquet_data(spark: SparkSession, path: str) -> DataFrame:
    """Lê dados Parquet e trata erros de leitura."""
    try:
        return spark.read.parquet(path)
    except Exception as e:
        logging.error(f"Erro ao ler o arquivo Parquet no caminho: {path}. Erro: {e}")
        raise

###########################################################################################################
# Função para validar DataFrames
###########################################################################################################
def validate_dataframes(dataframes: dict, layer: str) -> DataFrame:
    """
    Valida que todos os DataFrames possuem dados e realiza a união.
    """
    empty_sources = [source for source, df in dataframes.items() if df.count() == 0]

    if empty_sources:
        error_message = f"As seguintes fontes estão vazias na camada {layer}: {', '.join(empty_sources)}"
        logging.error(error_message)
        raise ValueError(error_message)

    logging.info(f"Todos os dados na camada {layer} foram validados com sucesso.")
    # União dos DataFrames
    df_all = dataframes['google_play'].union(dataframes['mongodb']).union(dataframes['apple_store'])
    return df_all, dataframes['google_play'], dataframes['mongodb'], dataframes['apple_store']

###########################################################################################################
# Validação do processo de carga
###########################################################################################################
def validate_source_load(spark: SparkSession, date_ref: str, type_processing: str):
    try:
        logging.info("Iniciando o processo de validação de dados.")

        if type_processing not in {"bronze", "silver"}:
            raise ValueError(f"Tipo de processamento '{type_processing}' inválido. Use 'bronze' ou 'silver'.")

        # Definindo caminhos para cada camada
        base_path = "/santander"
        paths = {
            "google_play": f"{base_path}/{type_processing}/compass/reviews/googlePlay/odate={date_ref}",
            "mongodb": f"{base_path}/{type_processing}/compass/reviews/mongodb/odate={date_ref}",
            "apple_store": f"{base_path}/{type_processing}/compass/reviews/appleStore/odate={date_ref}",
        }

        # Lendo os dados
        dataframes = {source: read_parquet_data(spark, path) for source, path in paths.items()}

        logging.info(f"Iniciando validação da camada '{type_processing}' para a data {date_ref}.")
        for source, df in dataframes.items():
            volume = df.count()
            logging.info(f"Fonte {source}: volumetria = {volume}")

        # Validando e unindo os DataFrames
        return validate_dataframes(dataframes, type_processing)

    except ValueError as ve:
        logging.error(f"Erro de validação: {ve}")
        raise
    except Exception as e:
        logging.error(f"Erro inesperado ao processar os dados: {e}")
        raise

###########################################################################################################
# Validação de schemas
###########################################################################################################
def validate_schema(spark: SparkSession, df: DataFrame, source_name: str) -> DataFrame:
    """
    Valida o esquema de um DataFrame comparando com o esquema esperado para uma fonte específica.
    Retorna um DataFrame com os resultados da validação e lança uma exceção em caso de erro.

    :param spark: Instância do SparkSession.
    :param df: DataFrame a ser validado.
    :param source_name: Nome da fonte (e.g., "google_play", "mongodb", "play_store").
    :return: DataFrame contendo os resultados da validação.
    :raises ValueError: Se o esquema do DataFrame for inválido ou a fonte for desconhecida.
    """
    # Dicionário de esquemas esperados
    schema_dict = {
        "google_play": google_play_schema(),
        "mongodb": mongodb_schema(),
        "play_store": play_store_schema()
    }

    # Dados iniciais para os resultados
    result_data = []

    # Verifica se a fonte fornecida está no dicionário de esquemas
    if source_name not in schema_dict:
        result_data.append({
            "source": source_name,
            "validation_status": "Unknown Source",
            "expected_schema": None,
            "current_schema": str(df.schema)
        })
        raise ValueError(f"Fonte desconhecida: {source_name}")

    expected_schema = schema_dict[source_name]
    validation_status = "Valid" if df.schema == expected_schema else "Invalid"

    result_data.append({
        "source": source_name,
        "validation_status": validation_status,
        "expected_schema": str(expected_schema),
        "current_schema": str(df.schema)
    })

    if validation_status == "Invalid":
        raise ValueError(
            f"Esquema inválido para a fonte '{source_name}'.\n"
            f"Expected Schema: {expected_schema}\n"
            f"Actual Schema: {df.schema}"
        )

    result_df = spark.createDataFrame(result_data)
    return result_df

###########################################################################################################
# Validação Pattern
###########################################################################################################
def validate_patterns(df: DataFrame, column_patterns: dict) -> DataFrame:
    """
    Valida as colunas do DataFrame com base nos padrões fornecidos.

    :param df: DataFrame a ser validado.
    :param column_patterns: Dicionário com os padrões de validação para cada coluna.
    :return: DataFrame contendo os resultados da validação.
    """
    validation_results = []

    for column, rules in column_patterns.items():
        if column in df.columns:
            column_validation = []

            if "type" in rules:
                column_type = rules["type"]
                if column_type == "string":
                    column_validation.append(df[column].cast("string").isNotNull())
                elif column_type == "int":
                    column_validation.append(df[column].cast("int").isNotNull())

            if "regex" in rules:
                regex_pattern = rules["regex"]
                column_validation.append(df[column].rlike(regex_pattern))

            if "not_null" in rules and rules["not_null"]:
                column_validation.append(df[column].isNotNull())

            if "min" in rules:
                column_validation.append(df[column] >= rules["min"])
            if "max" in rules:
                column_validation.append(df[column] <= rules["max"])

            combined_condition = F.when(F.and_(*column_validation), F.lit("Valid")).otherwise(F.lit("Invalid"))
            validation_results.append(combined_condition.alias(column))
        else:
            validation_results.append(F.lit(f"Coluna {column} não encontrada").alias(column))

    return df.select(*validation_results)

def execute_validation(spark: SparkSession, df: DataFrame):
    """
    Executa a validação das colunas do DataFrame utilizando os padrões definidos.

    :param spark: Instância do SparkSession.
    :param df: DataFrame a ser validado.
    """
    try:
        # Obter os padrões de validação
        column_patterns = get_column_patterns()

        # Validar o DataFrame com base nos padrões
        validation_result_df = validate_patterns(df, column_patterns)

        # Exibe o resultado da validação
        validation_result_df.show()

    except ValueError as ve:
        print(f"Erro de validação: {ve}")


def write_to_mongo(dados_feedback: dict, table_id: str, overwrite=False):
    """
    Escreve dados em uma coleção MongoDB, com a opção de sobrescrever a coleção.

    Args:
        dados_feedback (dict or list): Dados a serem inseridos na coleção (um único dicionário ou uma lista de dicionários).
        table_id (str): Nome da coleção onde os dados serão inseridos.
        overwrite (bool): Se True, sobrescreve a coleção, excluindo todos os documentos antes de inserir novos dados.
    """
    # Recuperar credenciais do MongoDB a partir das variáveis de ambiente
    mongo_user = os.environ["MONGO_USER"]
    mongo_pass = os.environ["MONGO_PASS"]
    mongo_host = os.environ["MONGO_HOST"]
    mongo_port = os.environ["MONGO_PORT"]
    mongo_db = os.environ["MONGO_DB"]

    # ---------------------------------------------- Escapar nome de usuário e senha ----------------------------------------------
    escaped_user = quote_plus(mongo_user)
    escaped_pass = quote_plus(mongo_pass)

    # ---------------------------------------------- Conexão com MongoDB ----------------------------------------------------------
    mongo_uri = f"mongodb://{escaped_user}:{escaped_pass}@{mongo_host}:{mongo_port}/{mongo_db}?authSource={mongo_db}&maxPoolSize=1"

    client = pymongo.MongoClient(mongo_uri)

    try:
        db = client[mongo_db]
        collection = db[table_id]

        # Se o parâmetro overwrite for True, exclui todos os documentos da coleção antes de inserir novos dados
        if overwrite:
            collection.delete_many({})

        if isinstance(dados_feedback, dict):
            collection.insert_one(dados_feedback)
        elif isinstance(dados_feedback, list):
            collection.insert_many(dados_feedback)
        else:
            raise ValueError("Os dados fornecidos não são válidos para inserção.")

    except Exception as e:
        logging.error(f"Erro ao salvar os dados no MongoDB: {e}")
        raise


def save_metrics(metrics_json: str):
    """
    Salva as métricas no MongoDB.
    """
    try:
        metrics_data = json.loads(metrics_json)
        write_to_mongo(metrics_data, "dt_datametrics_compass", overwrite=False)
        logging.info(f"Métricas da aplicação salvas: {metrics_json}")
    except json.JSONDecodeError as e:
        logging.error(f"Erro ao processar métricas: {e}", exc_info=True)
