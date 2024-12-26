import logging
import sys
from pyspark.sql import SparkSession
from datetime import datetime
from tools import *
from object_validate import *


# Configuração de logging
# Configuração básica do logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

def main():

    # Capturar argumentos da linha de comando
    args = sys.argv[1:]

    # Verificar se o número correto de argumentos foi passado
    if len(args) != 1:
        print("Usage: spark-submit app.py <type_processing> ")
        sys.exit(1)

    try:
        # Criação da sessão Spark
        with spark_session() as spark:

            logging.info("[*] Capturando as variaveis de entrada!")

            # CAPTURA ARGUMENTO DE ENTRADA #############################################################################
            type_processing = args[0]

            # PARAMETROS DE DATA E PATHS ###############################################################################
            datePath = datetime.now().strftime("%Y%m%d")
            path_rejeitados_schema = f"/santander/quality/compass/reviews/schema/odate={datePath}"
            path_rejeitados_pattern_google = f"/santander/quality/compass/reviews/pattern/google_play/odate={datePath}"
            path_rejeitados_pattern_apple= f"/santander/quality/compass/reviews/pattern/apple_store/odate={datePath}"
            path_rejeitados_pattern_mongo = f"/santander/quality/compass/reviews/pattern/internal_databases/odate={datePath}"

            ###########################################################################################################
            # Validação: Carga origem
            ###########################################################################################################

            logging.info("[*] Iniciando validacao da carga origem e volumetria!")
            if type_processing in {"bronze", "silver"}:
                try:
                    df_google_play, df_mongodb, df_apple_store = validate_source_load(spark, datePath, type_processing)

                    # Contagens das volumetrias
                    google_play_count = df_google_play.count()
                    mongodb_count = df_mongodb.count()
                    apple_store_count = df_apple_store.count()

                    # Construindo o texto estruturado
                    log_message = (
                        "\n"
                        "-------------------------------------------------------\n"
                        " [*] Validação da Carga Origem\n"
                        "-------------------------------------------------------\n"
                        f" > Tipo de Processamento   : {type_processing}\n"
                        f" > Volumetria Google Play  : {google_play_count}\n"
                        f" > Volumetria MongoDB      : {mongodb_count}\n"
                        f" > Volumetria Apple Store  : {apple_store_count}\n"
                        "-------------------------------------------------------"
                    )

                    # Logando a mensagem
                    logging.info(log_message)

                    if df_google_play.count() | df_mongodb.count() | df_apple_store.count() == 0:
                        logging.info("[*] Origem identificada vazia!")
                        sys.exit(1)

                except ValueError as e:
                    # Registra o erro com detalhes
                    logging.error(f"[*] Erro de validação de esquema: {e}")
            else:
                print(f"[*] {type_processing} não encontrado!")

            ###########################################################################################################
            # Validação: Schema
            ###########################################################################################################
            logging.info("[*] Validacao da Schema!")

            if type_processing == "bronze":
                try:
                    # Validação dos DataFrames com os respectivos esquemas
                    result_df_google_play = validate_schema(spark, df_google_play, "google_play_bronze")
                    result_df_mongodb = validate_schema(spark, df_mongodb, "mongodb_bronze")
                    result_df_apple_store = validate_schema(spark, df_apple_store, "apple_store_bronze")

                    # Combina todos os resultados em um único DataFrame
                    df_schema_all = result_df_google_play.union(result_df_mongodb).union(result_df_apple_store)

                    condition_df_schema_all_no_match = df_schema_all.filter(" validation_status = 'no_match' ")

                    if condition_df_schema_all_no_match.count() > 0:
                        df_schema_all.show(truncate=False)
                        save_dataframe(condition_df_schema_all_no_match, path_rejeitados_schema, "rejeitados_schema_bronze")
                        logging.error(f"[*] Erro de validação de esquema: {condition_df_schema_all_no_match.count()} rejeitados da bronze")
                        sys.exit(1)
                    else:
                        logging.info(f"[*] Não houve rejeitados para schema!")


                except ValueError as e:
                    # Registra o erro com detalhes
                    logging.error(f"[*] Erro de validação de esquema: {e}")

            elif type_processing == "silver":
                try:
                    # Validação dos DataFrames com os respectivos esquemas
                    result_df_google_play = validate_schema(spark, df_google_play, "google_play_silver")
                    result_df_mongodb = validate_schema(spark, df_mongodb, "mongodb_silver")
                    result_df_apple_store = validate_schema(spark, df_apple_store, "apple_store_silver")

                    # Combina todos os resultados em um único DataFrame
                    df_schema_all = result_df_google_play.union(result_df_mongodb).union(result_df_apple_store)

                    condition_df_schema_all_no_match = df_schema_all.filter(" validation_status = 'no_match' ")

                    if condition_df_schema_all_no_match.count() > 0:
                        df_schema_all.show(truncate=False)
                        save_dataframe(condition_df_schema_all_no_match, path_rejeitados_schema, "rejeitados_schema_silver")
                        logging.error(f"[*] Erro de validação de esquema: {condition_df_schema_all_no_match.count()} rejeitados da silver")
                        sys.exit(1)
                    else:
                        logging.info(f"[*] Não houve rejeitados para schema!")

                except ValueError as e:
                    # Registra o erro com detalhes
                    logging.error(f"[*] Erro de validação de esquema: {e}")
            else:
                print(f"[*] {type_processing} não encontrado!")



            ###########################################################################################################
            # Validação: Pattern
            ###########################################################################################################

            if type_processing == "silver":
                logging.info("[*] Iniciando validacao de pattern.")
                try:

                    # Escrita rejeitados Google #######################################################################
                    df_validated_googlePlay = validated_pattern_google_play(df_google_play)
                    df_googlePlay_no_match =  df_validated_googlePlay.filter(" validation = 'no_match' ")

                    count_rejeitados_google = df_googlePlay_no_match.count() > 1

                    if count_rejeitados_google:
                        print(f"[*]  Teve {df_googlePlay_no_match.count()} de rejeitados para o Google Play!")
                        df_googlePlay_no_match.show(5, truncate=False)
                        save_dataframe(df_googlePlay_no_match, path_rejeitados_pattern_google, "rejeitados_schema_google_silver")
                    else:
                        print("[*] Não teve rejeitados para o Google Play Silver!")

                    # Escrita rejeitados Mongodb #######################################################################
                    df_validated_mongodb = validated_pattern_mongodb(df_mongodb)
                    df_mongodb_no_match =  df_validated_mongodb.filter(" validation = 'no_match' ")

                    count_rejeitados_mongo = df_mongodb_no_match.count() > 1

                    if count_rejeitados_mongo > 1:
                        print(f"[*]  Teve {df_mongodb_no_match.count()} de rejeitados para o MongoDB!")
                        df_validated_mongodb.show(5, truncate=False)
                        save_dataframe(df_mongodb_no_match, path_rejeitados_pattern_mongo, "rejeitados_schema_internal_database_silver")
                    else:
                        print("[*] Não teve rejeitados para o MongoDB Silver!")

                    # Escrita rejeitados Apple #######################################################################
                    df_validated_appleStore = validated_pattern_apple_store(df_apple_store)
                    df_appleStore_no_match =  df_validated_appleStore.filter(" validation = 'no_match' ")

                    count_rejeitados_apple = df_appleStore_no_match.count() > 1

                    if count_rejeitados_apple > 1:
                        print(f"[*] Teve {df_appleStore_no_match.count()} de rejeitados para a Apple Store!")
                        df_appleStore_no_match.show(5, truncate=False)
                        save_dataframe(df_appleStore_no_match, path_rejeitados_pattern_apple, "rejeitados_schema_apple_silver")
                    else:
                        print("[*] Não teve rejeitados para a Apple Store Silver!")

                    if count_rejeitados_google or count_rejeitados_mongo or count_rejeitados_apple > 1:
                        # Encerra o script com erro
                        sys.exit(1)

                except ValueError as e:
                    # Registra o erro com detalhes
                    logging.error(f"[*] Erro de validação do pattern: {e}")


    except Exception as e:
        logging.error(f"[*] An error occurred: {e}", exc_info=True)
        

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
