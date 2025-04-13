from pyspark.sql import SparkSession, DataFrame
from logging_utils import log_structured
from elasticsearch import Elasticsearch

def create_spark_session() -> SparkSession:
    try:
        spark = SparkSession.builder \
            .config("spark.jars.packages", "org.apache.spark:spark-measure_2.12:0.16") \
            .config("spark.sql.parquet.enableVectorizedReader", "false") \
            .getOrCreate()
        log_structured(level="INFO", message="Sessao Spark criada com sucesso")
        return spark
    except Exception as e:
        log_structured(level="ERROR", message="Falha ao criar sessao Spark", details={"error": str(e)})
        raise

def read_parquet_data(spark: SparkSession, path: str) -> DataFrame:
    try:
        return spark.read.parquet(path)
    except Exception as e:
        log_structured(level="ERROR", message="Erro ao ler arquivo Parquet", details={"path": path, "error": str(e)})
        raise

def save_dataframe(df: DataFrame, path: str, label: str):
    try:
        if df.count() > 0:
            df.write.mode("overwrite").parquet(path)
            log_structured(level="INFO", message=f"Dados salvos em {path}", details={"label": label})
        else:
            log_structured(level="WARNING", message=f"Nenhum dado para salvar em {path}", details={"label": label})
    except Exception as e:
        log_structured(level="ERROR", message="Erro ao salvar DataFrame", details={"path": path, "error": str(e)})
        raise