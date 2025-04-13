import sys
from datetime import datetime
from pyspark.sql import SparkSession
from config import Config
from validation import DataValidator
from logging_utils import setup_logging, log_structured
from spark_utils import create_spark_session
from elasticsearch_utils import save_metrics_job_fail

def main():
    # Configuração inicial
    setup_logging()
    config = Config()

    # Capturar argumentos da linha de comando
    if len(sys.argv) != 3:
        log_structured(
            level="ERROR",
            message="Numero incorreto de argumentos",
            details="Usage: spark-submit app.py <env> <type_processing>"
        )
        sys.exit(1)

    env, type_processing = sys.argv[1], sys.argv[2]

    # Inicializar Spark
    spark = create_spark_session()

    try:
        log_structured(level="INFO", message="Iniciando processamento", details={"type_processing": type_processing})

        # Validação de carga e schema
        validator = DataValidator(spark, config, type_processing)
        df_google_play, df_mongodb, df_apple_store = validator.validate_source_load()

        if type_processing in {"bronze", "silver"}:
            validator.validate_schemas(df_google_play, df_mongodb, df_apple_store)

        if type_processing == "silver":
            validator.validate_patterns(df_google_play, df_mongodb, df_apple_store)

        log_structured(level="INFO", message="Processamento concluido com sucesso")

    except Exception as e:
        log_structured(level="ERROR", message="Erro durante o processamento", details={"error": str(e)})
        save_metrics_job_fail({"error": str(e), "timestamp": datetime.now().isoformat()})
        sys.exit(1)

if __name__ == "__main__":
    main()