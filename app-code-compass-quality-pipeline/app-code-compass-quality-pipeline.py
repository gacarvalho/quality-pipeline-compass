import logging
from pyspark.sql import SparkSession
from datetime import datetime
from tools import *

# Configuração de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    try:
        # Criação da sessão Spark
        with spark_session() as spark:

            datePath = datetime.now().strftime("%Y%m%d")

            ###########################################################################################################
            # Validação: Carga origem
            ###########################################################################################################
            df_validate_all_sources, df_google_play, df_mongodb, df_apple_store = validate_source_load(spark, datePath, "args")
            df_validate_all_sources.show(truncate=False)

            ###########################################################################################################
            # Validação: Schema
            ###########################################################################################################
            # try:
            #     result_df_google_play = validate_schema(spark, df_google_play, "google_play")
            #     result_df_mongodb = validate_schema(spark, df_mongodb, "mongodb")
            #     result_df_apple_store = validate_schema(spark, df_apple_store, "google_play")
            #
            #     df_schema_all = result_df_google_play.union(result_df_mongodb).union(result_df_apple_store)
            #     df_schema_all.show(truncate=False)
            #
            # except ValueError as e:
            #     logging.error(f"Erro de validação de esquema: {e}")


            ###########################################################################################################
            # Validação: Pattern
            ###########################################################################################################
            # execute_validation(spark, df_google_play)
            # execute_validation(spark, df_mongodb)
            # execute_validation(spark, df_apple_store)
            # Salvando dados e métricas
            # save_data(valid_df, invalid_df)

    except Exception as e:
        logging.error(f"An error occurred: {e}", exc_info=True)
        

def spark_session():
    """
    Cria e retorna uma sessão Spark.
    """
    try:
        spark = SparkSession.builder \
            .appName("App Reviews [google play]") \
            .config("spark.jars.packages", "org.apache.spark:spark-measure_2.12:0.16") \
            .config("spark.sql.parquet.enableVectorizedReader", "false") \
            .getOrCreate()
        return spark
    except Exception as e:
        logging.error(f"Failed to create SparkSession: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    main()
