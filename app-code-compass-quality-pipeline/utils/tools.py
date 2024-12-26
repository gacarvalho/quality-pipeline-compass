import os, json, logging, sys, requests, unicodedata, pymongo
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    regexp_extract, col, when, lit, array, size, expr
)
from pyspark.sql.types import (
    MapType, StringType, ArrayType, BinaryType, StructType, StructField, DoubleType, LongType
)
from pathlib import Path
from urllib.parse import quote_plus
from object_validate import *

###########################################################################################################
# Validação leitura da origem e carga
###########################################################################################################
def read_parquet_data(spark: SparkSession, path: str) -> DataFrame:
    """Lê dados Parquet e trata erros de leitura."""
    try:
        return spark.read.parquet(path)
    except Exception as e:
        logging.error(f"[*] Erro ao ler o arquivo Parquet no caminho: {path}. Erro: {e}")
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
        error_message = f"[*] As seguintes fontes estão vazias na camada {layer}: {', '.join(empty_sources)}"
        logging.error(error_message)
        raise ValueError(error_message)

    logging.info(f"[*] Todos os dados na camada {layer} foram validados com sucesso.")

    if layer == "bronze":
        dataframes['google_play'] = dataframes['google_play'].drop("response")

    # União dos DataFrames
    return dataframes['google_play'].drop("response"), dataframes['mongodb'], dataframes['apple_store']

###########################################################################################################
# Validação do processo de carga
###########################################################################################################
def validate_source_load(spark: SparkSession, date_ref: str, type_processing: str):
    try:
        logging.info("[*] Iniciando o processo de validação de dados.")

        if type_processing not in {"bronze", "silver"}:
            raise ValueError(f"[*] Tipo de processamento '{type_processing}' inválido. Use 'bronze' ou 'silver'.")

        """ Definindo caminhos para cada camada
        Variável wildcard:

            - Para "bronze", wildcard recebe o valor "*/".
            - Para "silver", wildcard recebe uma string vazia ("").

        Caminhos em paths:
            - Substituí */ por {wildcard} nos caminhos das fontes.
            - Isso permite que o comportamento do caminho mude dinamicamente conforme o valor de type_processing.

        Exemplo de Caminhos Gerados:
            - Se type_processing for "bronze" e date_ref for "2024-12-08", os caminhos serão:
            
                /santander/bronze/compass/reviews/googlePlay/*/odate=2024-12-08
                
            - Se type_processing for "silver" e date_ref for "2024-12-08", os caminhos serão:            
                /santander/silver/compass/reviews/googlePlay/odate=2024-12-08
        """

        base_path = "/santander"
        wildcard = "*/" if type_processing == "bronze" else ""
        paths = {
            "google_play": f"{base_path}/{type_processing}/compass/reviews/googlePlay/{wildcard}odate={date_ref}",
            "mongodb": f"{base_path}/{type_processing}/compass/reviews/mongodb/{wildcard}odate={date_ref}",
            "apple_store": f"{base_path}/{type_processing}/compass/reviews/appleStore/{wildcard}odate={date_ref}",
        }

        # Lendo os dados
        dataframes = {source: read_parquet_data(spark, path) for source, path in paths.items()}

        logging.info(f"[*] Iniciando validação da camada '{type_processing}' para a data {date_ref}.")


        # Validando e unindo os DataFrames
        return validate_dataframes(dataframes, type_processing)

    except ValueError as ve:
        logging.error(f"[*] Erro de validação: {ve}")
        raise
    except Exception as e:
        logging.error(f"[*] Erro inesperado ao processar os dados: {e}")
        raise

###########################################################################################################
# Validação de schemas
###########################################################################################################
def simplify_schema(schema):
    """
    Simplifica o esquema, tratando tipos compostos e garantindo a comparação.
    """
    if isinstance(schema, StructType):
        simplified_fields = []
        for field in schema.fields:
            simplified_fields.append(StructField(field.name, simplify_schema(field.dataType), field.nullable))
        return StructType(simplified_fields)

    elif isinstance(schema, ArrayType):
        # Para ArrayType, simplifica o tipo do elemento
        return ArrayType(simplify_schema(schema.elementType))

    else:
        # Para tipos simples (como StringType), retorna o tipo diretamente
        return schema

def compare_schemas(actual_schema, expected_schema):
    """
    Compara dois esquemas, levando em conta tipos compostos.
    """
    # Simplifica os esquemas antes de comparar
    simplified_actual = simplify_schema(actual_schema)
    simplified_expected = simplify_schema(expected_schema)

    # Compara os esquemas simplificados
    return simplified_actual == simplified_expected

def validate_schema(spark, df, source_name):
    """
    Valida o esquema de um DataFrame comparando com o esquema esperado para uma fonte específica.
    Retorna um DataFrame com os resultados da validação e lança uma exceção em caso de erro.
    """
    # Dicionário de esquemas esperados
    schema_dict = {
        # bronze
        "google_play_bronze": google_play_schema_bronze(),
        "mongodb_bronze": mongodb_schema_bronze(),
        "apple_store_bronze": apple_store_schema_bronze(),
        # silver
        "google_play_silver": google_play_schema_silver(),
        "mongodb_silver": mongodb_schema_silver(),
        "apple_store_silver": apple_store_schema_silver(),
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

    # Compara os esquemas de forma robusta
    validation_status = "match" if compare_schemas(df.schema, expected_schema) else "no_match"

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

def validated_pattern_google_play(df: DataFrame) -> DataFrame:
    """
    Valida o DataFrame com base na estrutura e padrões fornecidos.

    Parameters:
        df (DataFrame): DataFrame a ser validado.

    Returns:
        DataFrame: DataFrame com as colunas "validation" e "failed_Columns".
    """
    # Condições para validação
    id_condition = ~col("id").rlike(r"^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$")
    app_condition = ~col("app").rlike(r"^[a-z0-9\-]+$")
    rating_condition = ~col("rating").cast("string").rlike(r"^[1"r"-5](\.\d+)?$")
    iso_date_condition = ~col("iso_date").rlike(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
    title_condition = ~col("title").rlike(r"^.+$")
    snippet_condition = ~col("snippet").rlike(r"^.+$")

    # Condição para validar `historical_data`
    historical_data_condition = when(
        size(col("historical_data")) == 0,
        lit(True)  # Falha se não há dados em `historical_data`
    ).otherwise(lit(False))

    # Criação da coluna de falhas
    df = df.withColumn(
        "failed_Columns",
        array(
            when(id_condition, lit("id")).otherwise(lit(None)),
            when(app_condition, lit("app")).otherwise(lit(None)),
            when(rating_condition, lit("rating")).otherwise(lit(None)),
            when(iso_date_condition, lit("iso_date")).otherwise(lit(None)),
            when(title_condition, lit("title")).otherwise(lit(None)),
            when(snippet_condition, lit("snippet")).otherwise(lit(None)),
            when(historical_data_condition, lit("historical_data")).otherwise(lit(None)),
        )
    ).withColumn(
        "failed_Columns",
        expr("filter(failed_Columns, x -> x IS NOT NULL)")  # Remove valores nulos
    )

    # Adiciona a coluna "validation"
    df = df.withColumn(
        "validation",
        when(size(col("failed_Columns")) == 0, lit("match"))
        .otherwise(lit("no_match"))
    )

    return df

def validated_pattern_apple_store(df: DataFrame) -> DataFrame:
    """
    Valida o DataFrame com base na estrutura e padrões fornecidos para a Apple Store.

    Parameters:
        df (DataFrame): DataFrame a ser validado.

    Returns:
        DataFrame: DataFrame com as colunas "validation" e "failed_columns".
    """
    # Condições para validação
    id_condition = ~col("id").rlike(r"^^\d+$")
    name_client_condition = ~col("name_client").rlike(r"^.+$")
    app_condition = ~col("app").rlike(r"^[a-zA-Z0-9\-]+$")
    im_version_condition = ~col("im_version").rlike(r"^\d+(\.\d+)*$")
    im_rating_condition = ~col("im_rating").cast("string").rlike(r"^[0-5](\.\d+)?$")
    title_condition = ~col("title").rlike(r"^.+$")
    content_condition = col("content").isNull()

    # Criação da coluna de falhas
    df = df.withColumn(
        "failed_columns",
        array(
            when(id_condition, lit("id")).otherwise(lit(None)),
            when(name_client_condition, lit("name_client")).otherwise(lit(None)),
            when(app_condition, lit("app")).otherwise(lit(None)),
            when(im_version_condition, lit("im_version")).otherwise(lit(None)),
            when(im_rating_condition, lit("im_rating")).otherwise(lit(None)),
            when(title_condition, lit("title")).otherwise(lit(None)),
            when(content_condition, lit("content")).otherwise(lit(None))
        )
    ).withColumn(
        "failed_columns",
        expr("filter(failed_columns, x -> x IS NOT NULL)")  # Remove valores nulos
    )

    # Adiciona a coluna "validation"
    df = df.withColumn(
        "validation",
        when(size(col("failed_columns")) == 0, lit("match"))
        .otherwise(lit("no_match"))
    )

    return df

def validated_pattern_mongodb(df: DataFrame) -> DataFrame:
    """
    Valida o DataFrame com base na estrutura e padrões fornecidos para o MongoDB.

    Parameters:
        df (DataFrame): DataFrame a ser validado.

    Returns:
        DataFrame: DataFrame com as colunas "validation" e "failed_columns".
    """
    # Condições para validação
    id_condition = ~col("id").rlike(r"^[a-f0-9]+$")
    customer_id_condition = col("customer_id").isNull()
    cpf_condition = ~col("cpf").rlike(r"^\d{11}$|^\d{3}\.\d{3}\.\d{3}-\d{2}$")
    app_condition = ~col("app").rlike(r"^[a-zA-Z0-9\-]+$")
    rating_condition = ~col("rating").cast("string").rlike(r"^[0-5](\.\d+)?$")
    timestamp_condition = ~col("timestamp").rlike(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?$")
    comment_condition = col("comment").isNull()
    app_version_condition = ~col("app_version").rlike(r"^\d+(\.\d+)*$")
    os_version_condition = ~col("os_version").rlike(r"^\d+(\.\d+)*$")
    os_condition = col("os").isNull()

    # Criação da coluna de falhas
    df = df.withColumn(
        "failed_columns",
        array(
            when(id_condition, lit("id")).otherwise(lit(None)),
            when(customer_id_condition, lit("customer_id")).otherwise(lit(None)),
            when(cpf_condition, lit("cpf")).otherwise(lit(None)),
            when(app_condition, lit("app")).otherwise(lit(None)),
            when(rating_condition, lit("rating")).otherwise(lit(None)),
            when(timestamp_condition, lit("timestamp")).otherwise(lit(None)),
            when(comment_condition, lit("comment")).otherwise(lit(None)),
            when(app_version_condition, lit("app_version")).otherwise(lit(None)),
            when(os_version_condition, lit("os_version")).otherwise(lit(None)),
            when(os_condition, lit("os")).otherwise(lit(None))
        )
    ).withColumn(
        "failed_columns",
        expr("filter(failed_columns, x -> x IS NOT NULL)")  # Remove valores nulos
    )

    # Adiciona a coluna "validation"
    df = df.withColumn(
        "validation",
        when(size(col("failed_columns")) == 0, lit("match"))
        .otherwise(lit("no_match"))
    )

    return df


def save_reviews(reviews_df: DataFrame, directory: str):
    """
    Salva os dados do DataFrame no formato Delta no diretório especificado.

    Args:
        reviews_df (DataFrame): DataFrame PySpark contendo as avaliações.
        directory (str): Caminho do diretório onde os dados serão salvos.
    """
    try:
        # Verifica se o diretório existe e cria-o se não existir
        Path(directory).mkdir(parents=True, exist_ok=True)

        # Escrever os dados no formato Delta
        # reviews_df.write.format("delta").mode("overwrite").save(directory)
        reviews_df.write.option("compression", "snappy").mode("overwrite").parquet(directory)
        logging.info(f"[*] Dados salvos em {directory} no formato Delta")
    except Exception as e:
        logging.error(f"[*] Erro ao salvar os dados: {e}")
        sys.exit(1)

def save_dataframe(df, path, label):
    """
    Salva o DataFrame em formato parquet e loga a operação.
    """
    try:
        if df.limit(1).count() > 0:  # Verificar existência de dados
            logging.info(f"[*] Salvando dados {label} para: {path}")
            df.printSchema()
            save_reviews(df, path)
        else:
            logging.warning(f"[*] Nenhum dado {label} foi encontrado!")
    except Exception as e:
        logging.error(f"[*] Erro ao salvar dados {label}: {e}", exc_info=True)


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
            raise ValueError("[*] Os dados fornecidos não são válidos para inserção.")

    except Exception as e:
        logging.error(f"[*] Erro ao salvar os dados no MongoDB: {e}")
        raise


def save_metrics(metrics_json: str):
    """
    Salva as métricas no MongoDB.
    """
    try:
        metrics_data = json.loads(metrics_json)
        write_to_mongo(metrics_data, "dt_datametrics_compass", overwrite=False)
        logging.info(f"[*] Métricas da aplicação salvas: {metrics_json}")
    except json.JSONDecodeError as e:
        logging.error(f"[*] Erro ao processar métricas: {e}", exc_info=True)
