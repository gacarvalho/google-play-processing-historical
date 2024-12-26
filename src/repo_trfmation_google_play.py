import logging
import json
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import input_file_name, regexp_extract
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, MapType
from datetime import datetime
try:
    # Obtem import para cenarios de execuções em ambiente PRE, PRD
    from tools import *
    from metrics import MetricsCollector, validate_ingest
except ModuleNotFoundError:
    # Obtem import para cenarios de testes unitarios
    from src.utils.tools import *
    from src.metrics.metrics import MetricsCollector, validate_ingest



# Configuração de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    try:
        
        # Criação da sessão Spark
        with spark_session() as spark:
            
            # Coleta de métricas
            metrics_collector = MetricsCollector(spark)
            metrics_collector.start_collection()

            # Definindo caminhos
            datePath = datetime.now().strftime("%Y%m%d")
            pathSource = "/santander/bronze/compass/reviews/googlePlay/*/*"
            path_target = f"/santander/silver/compass/reviews/googlePlay/odate={datePath}/"
            path_target_fail = f"/santander/silver/compass/reviews_fail/googlePlay/odate={datePath}/"

            # Definindo o schema para o DataFrame
            schema = define_schema()

            # Leitura do arquivo Parquet
            df_read = read_data(spark, schema, pathSource)

            # Processamento dos dados
            df_processado = processamento_reviews(df_read)


            # Validação e separação dos dados
            valid_df, invalid_df, validation_results = validate_ingest(spark, df_processado)

            valid_df.printSchema()
            invalid_df.printSchema()

            # Coleta de métricas após processamento
            metrics_collector.end_collection()
            metrics_json = metrics_collector.collect_metrics(valid_df, invalid_df, validation_results, "silver_google_play")

            # Salvando dados e métricas
            save_data(valid_df, invalid_df, path_target, path_target_fail)
            save_metrics(metrics_json)

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
        logging.error(f"[*] Failed to create SparkSession: {e}", exc_info=True)
        raise

def define_schema() -> StructType:
    """
    Define o schema para os dados dos reviews.
    """
    return StructType([
        StructField("avatar", StringType(), True),
        StructField("date", StringType(), True),
        StructField("id", StringType(), True),
        StructField("iso_date", StringType(), True),
        StructField("likes", LongType(), True),
        StructField("rating", DoubleType(), True),
        StructField("response", MapType(StringType(), StringType(), True), True),
        StructField("snippet", StringType(), True),
        StructField("title", StringType(), True)
    ])



def save_data(valid_df: DataFrame, invalid_df: DataFrame, path_target: str, path_target_fail: str):
    """
    Salva os dados válidos e inválidos nos caminhos apropriados.
    """
    try:
        save_dataframe(valid_df, path_target, "valido")
        save_dataframe(invalid_df, path_target_fail, "invalido")
    except Exception as e:
        logging.error(f"[*] Erro ao salvar os dados: {e}", exc_info=True)
        raise

def save_metrics(metrics_json: str):
    """
    Salva as métricas no MongoDB.
    """
    try:
        metrics_data = json.loads(metrics_json)
        write_to_mongo(metrics_data, "dt_datametrics_compass")
        logging.info(f"[*] Métricas da aplicação salvas: {metrics_json}")
    except json.JSONDecodeError as e:
        logging.error(f"[*] Erro ao processar métricas: {e}", exc_info=True)

if __name__ == "__main__":
    main()
