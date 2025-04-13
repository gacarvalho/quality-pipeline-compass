from pyspark.sql import DataFrame, SparkSession
from config import Config
from logging_utils import log_structured
from spark_utils import read_parquet_data, save_dataframe
from object_validate import (
    google_play_schema_bronze, mongodb_schema_bronze, apple_store_schema_bronze,
    google_play_schema_silver, mongodb_schema_silver, apple_store_schema_silver
)
from typing import Tuple
from datetime import datetime
from pyspark.sql.functions import col, when, lit, array, expr, size
from elasticsearch import Elasticsearch

class DataValidator:
    def __init__(self, spark: SparkSession, config: Config, type_processing: str):
        self.spark = spark
        self.config = config
        self.type_processing = type_processing

    def validate_source_load(self) -> Tuple[DataFrame, DataFrame, DataFrame]:
        """
        Valida a carga dos dados.

        :return: Tupla com os DataFrames do Google Play, MongoDB e Apple Store.
        :raises ValueError: Se alguma fonte de dados estiver vazia.
        """
        log_structured(level="INFO", message="Validando carga de origem")

        try:
            date_path = datetime.now().strftime("%Y%m%d")
            base_path = self.config.base_path
            wildcard = "*/" if self.type_processing == "bronze" else ""

            paths = {
                "google_play": f"{base_path}/{self.type_processing}/compass/reviews/googlePlay/{wildcard}odate={date_path}",
                "mongodb": f"{base_path}/{self.type_processing}/compass/reviews/mongodb/{wildcard}odate={date_path}",
                "apple_store": f"{base_path}/{self.type_processing}/compass/reviews/appleStore/{wildcard}odate={date_path}",
            }

            # Ler os DataFrames
            df_google_play = read_parquet_data(self.spark, paths["google_play"])
            df_mongodb = read_parquet_data(self.spark, paths["mongodb"])
            df_apple_store = read_parquet_data(self.spark, paths["apple_store"])

            # Validar volumetria
            if df_google_play.count() == 0 or df_mongodb.count() == 0 or df_apple_store.count() == 0:
                error_message = "Origem identificada vazia!"
                log_structured(level="ERROR", message=error_message)
                raise ValueError(error_message)

            return df_google_play, df_mongodb, df_apple_store

        except Exception as e:
            log_structured(level="ERROR", message="Erro ao validar carga de origem", details={"error": str(e)})
            raise

    def validate_schemas(self, df_google_play: DataFrame, df_mongodb: DataFrame, df_apple_store: DataFrame):
        """
        Valida os schemas dos DataFrames.

        :param df_google_play: DataFrame do Google Play.
        :param df_mongodb: DataFrame do MongoDB.
        :param df_apple_store: DataFrame da Apple Store.
        """
        log_structured(level="INFO", message="Validando schemas")

        # Instancia o SchemaValidator corretamente
        schema_validator = SchemaValidator(self.spark)
        sources = {
            "google_play": df_google_play,
            "mongodb": df_mongodb,
            "apple_store": df_apple_store,
        }

        for source_name, df in sources.items():
            result = schema_validator.validate_schema(df, source_name, self.type_processing)
            if result.filter("validation_status = 'no_match'").count() > 0:
                self._save_rejected_data(result, self.config.path_rejeitados_schema, f"rejeitados_schema_{source_name}")

    def validate_patterns(self, df_google_play: DataFrame, df_mongodb: DataFrame, df_apple_store: DataFrame):
        """
        Valida os padrões dos DataFrames.
        """
        log_structured(level="INFO", message="Validando padrões")

        # Instancia o PatternValidator
        pattern_validator = PatternValidator(self.spark)

        # Valida os padrões para cada DataFrame
        df_google_play = pattern_validator.validate_google_play(df_google_play)
        df_mongodb = pattern_validator.validate_mongodb(df_mongodb)
        df_apple_store = pattern_validator.validate_apple_store(df_apple_store)

        # Salva os dados rejeitados, se houver
        self._save_rejected_data(
            df_google_play.filter(col("validation") == "no_match"),
            self.config.path_rejeitados_pattern_google,
            "rejeitados_patterns_google_play"
        )
        self._save_rejected_data(
            df_mongodb.filter(col("validation") == "no_match"),
            self.config.path_rejeitados_pattern_mongo,
            "rejeitados_patterns_mongodb"
        )
        self._save_rejected_data(
            df_apple_store.filter(col("validation") == "no_match"),
            self.config.path_rejeitados_pattern_apple,
            "rejeitados_patterns_apple_store"
        )

    def _save_rejected_data(self, df: DataFrame, path: str, label: str):
        """
        Salva os dados rejeitados.

        :param df: DataFrame com os dados rejeitados.
        :param path: Caminho onde os dados serão salvos.
        :param label: Rótulo para identificação no log.
        """
        if df.count() > 0:
            save_dataframe(df, path, label)
            df.show()
            df.show(truncate=False)
        else:
            log_structured(level="INFO", message=f"Nenhum dado rejeitado para {label}")

class SchemaValidator:
    """
    Classe interna para validação de schemas.
    """
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def validate_schema(self, df: DataFrame, source_name: str, type_processing: str) -> DataFrame:
        """
        Valida o schema de um DataFrame.

        :param df: DataFrame a ser validado.
        :param source_name: Nome da fonte de dados (google_play, mongodb, apple_store).
        :param type_processing: Tipo de processamento (bronze ou silver).
        :return: DataFrame com os resultados da validação.
        """
        # Dicionário de schemas esperados
        schema_dict = {
            "google_play_bronze": google_play_schema_bronze(),
            "mongodb_bronze": mongodb_schema_bronze(),
            "apple_store_bronze": apple_store_schema_bronze(),
            "google_play_silver": google_play_schema_silver(),
            "mongodb_silver": mongodb_schema_silver(),
            "apple_store_silver": apple_store_schema_silver(),
        }

        # Verifica se a fonte fornecida está no dicionário de schemas
        key = f"{source_name}_{type_processing}"
        if key not in schema_dict:
            raise ValueError(f"Schema desconhecido para a fonte: {key}")

        expected_schema = schema_dict[key]

        # Compara os schemas
        validation_status = "match" if df.schema == expected_schema else "no_match"

        # Cria um DataFrame com os resultados da validação
        result_data = [{
            "source": source_name,
            "validation_status": validation_status,
            "expected_schema": str(expected_schema),
            "current_schema": str(df.schema)
        }]

        return self.spark.createDataFrame(result_data)

class PatternValidator:
    """
    Classe interna para validação de padrões.
    """
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def validate_google_play(self, df: DataFrame) -> DataFrame:
        """
        Valida os padrões do DataFrame do Google Play.
        Retorna um DataFrame com as colunas "validation" e "failed_columns".
        """
        # Condições para validação
        id_condition = ~col("id").rlike(r"^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$")
        app_condition = ~col("app").rlike(r"^[a-z0-9\-_]+$")
        rating_condition = ~col("rating").cast("string").rlike(r"^[1-5](\.\d+)?$")
        iso_date_condition = ~col("iso_date").rlike(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
        title_condition = ~col("title").rlike(r"^.+$")
        snippet_condition = ~col("snippet").rlike(r"^.+$")

        # Criação da coluna de falhas
        df = df.withColumn(
            "failed_columns",
            array(
                when(id_condition, lit("id")).otherwise(lit(None)),
                when(app_condition, lit("app")).otherwise(lit(None)),
                when(rating_condition, lit("rating")).otherwise(lit(None)),
                when(iso_date_condition, lit("iso_date")).otherwise(lit(None)),
                when(title_condition, lit("title")).otherwise(lit(None)),
                when(snippet_condition, lit("snippet")).otherwise(lit(None))
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

    def validate_mongodb(self, df: DataFrame) -> DataFrame:
        """
        Valida os padrões do DataFrame do MongoDB.
        Retorna um DataFrame com as colunas "validation" e "failed_columns".
        """
        # Condições para validação
        id_condition = ~col("id").rlike(r"^[a-f0-9]+$")
        customer_id_condition = col("customer_id").isNull()
        cpf_condition = ~col("cpf").rlike(r"^\d{11}$|^\d{3}\.\d{3}\.\d{3}-\d{2}$")
        app_condition = ~col("app").rlike(r"^[a-z0-9\-_]+$")
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

    def validate_apple_store(self, df: DataFrame) -> DataFrame:
        """
        Valida os padrões do DataFrame da Apple Store.
        Retorna um DataFrame com as colunas "validation" e "failed_columns".
        """
        # Condições para validação
        id_condition = ~col("id").rlike(r"^\d+$")
        name_client_condition = ~col("name_client").rlike(r"^.+$")
        app_condition = ~col("app").rlike(r"^[a-z0-9\-_]+$")
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