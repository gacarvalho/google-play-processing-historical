import os
import subprocess
import json
import logging
import pymongo
from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.functions import col, lit, to_date, when, input_file_name, regexp_extract
from pyspark.sql.utils import AnalysisException
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    LongType,
    ArrayType,
    IntegerType
)
from datetime import datetime
from pathlib import Path
from urllib.parse import quote_plus
from unidecode import unidecode
from elasticsearch import Elasticsearch
try:
    # Obtem import para cenarios de execuções em ambiente PRE, PRD
    from schema_google import google_play_schema_silver
except ModuleNotFoundError:
    # Obtem import para cenarios de testes unitarios
    from src.schemas.schema_google import google_play_schema_silver


# Função para remover acentos
def remove_accents(s):
    return unidecode(s)

remove_accents_udf = F.udf(remove_accents, StringType())

def log_error(e, df):
    """Gera e salva métricas de erro no Elastic."""

    # Convertendo "segmento" para uma lista de strings
    segmentos_unicos = [row["segmento"] for row in df.select("segmento").distinct().collect()]

    error_metrics = {
        "timestamp": datetime.now().isoformat(),
        "layer": "silver",
        "project": "compass",
        "job": "google_play_reviews",
        "priority": "0",
        "tower": "SBBR_COMPASS",
        "client": segmentos_unicos,
        "error": str(e)
    }

    # Serializa para JSON e salva no MongoDB
    save_metrics_job_fail(json.dumps(error_metrics))

def read_source_parquet(spark, schema, path):
    """Tenta ler um Parquet e retorna None se não houver dados"""
    try:
        df = spark.read.schema(schema).parquet(path)
        if df.isEmpty():
            print(f"[*] Nenhum dado encontrado em: {path}")
            return None
        return df.withColumn("app", regexp_extract(input_file_name(), "/googlePlay/(.*?)/odate=", 1)) \
                 .drop("response") \
                 .withColumn("segmento", regexp_extract(input_file_name(), r"/googlePlay/[^/_]+_([pfj]+)/odate=", 1))
    except AnalysisException:
        print(f"[*] Falha ao ler: {path}. O arquivo pode não existir.")
        return None

def processamento_reviews(df: DataFrame):

    logging.info(f"{datetime.now().strftime('%Y%m%d %H:%M:%S.%f')} [*] Processando o tratamento da camada historica")

    # Aplicando as transformações no DataFrame
    df_select = df.select(
        "avatar",
        "id",
        "iso_date",
        "app",
        "segmento",
        col("rating").cast("int").cast("string").alias("rating"),
        "likes",
        F.upper(remove_accents_udf(F.col("title"))).alias("title"),
        F.upper(remove_accents_udf(F.col("snippet"))).alias("snippet")
    )

    return df_select


def get_schema(df, schema):
    """
    Obtém o DataFrame a seguir o schema especificado.
    """
    for field in schema.fields:
        if field.dataType == IntegerType():
            df = df.withColumn(field.name, df[field.name].cast(IntegerType()))
        elif field.dataType == StringType():
            df = df.withColumn(field.name, df[field.name].cast(StringType()))
    return df.select([field.name for field in schema.fields])



def save_dataframe(df, path, label):
    """
    Salva o DataFrame em formato parquet e loga a operação.
    """
    try:
        # Verifica se a coluna "historical_data" existe no DataFrame
        if "historical_data" not in df.columns:
            # Adiciona a coluna "historical_data" com um valor padrão
            df = df.withColumn(
                "historical_data",
                lit(None).cast("array<struct<title:string,snippet:string,app:string,rating:string,iso_date:string>>")
            )


        schema = google_play_schema_silver()
        # Alinhar o DataFrame ao schema definido
        df = get_schema(df, schema)

        if df.limit(1).count() > 0:  # Verificar existência de dados
            logging.info(f"[*] Salvando dados {label} para: {path}")
            # Verifica se o diretório existe e cria-o se não existir
            Path(path).mkdir(parents=True, exist_ok=True)

            df.write.option("compression", "snappy").mode("overwrite").parquet(path)
            logging.info(f"[*] Dados salvos em {path} no formato Parquet")
        else:
            logging.warning(f"[*] Nenhum dado {label} foi encontrado!")
    except Exception as e:
        logging.error(f"[*] Erro ao salvar dados {label}: {e}", exc_info=True)
        log_error(e, df)


def path_exists() -> bool:

    # Caminho para os dados históricos
    historical_data_path = "/santander/silver/compass/reviews/googlePlay/"

    # Verificando se o caminho existe no HDFS
    hdfs_path_exists = os.system(f"hadoop fs -test -e {historical_data_path} ") == 0

    if not hdfs_path_exists:
        print(f"[*] O caminho {historical_data_path} não existe no HDFS.")
        return False  # Retorna False se o caminho não existir no HDFS

    try:
        # Comando para listar os diretórios no HDFS
        cmd = f"hdfs dfs -ls {historical_data_path}"

        # Executar o comando HDFS
        result = subprocess.run(cmd.split(), capture_output=True, text=True, check=True)

        # Verificar se há partições "odate="
        if "odate=" in result.stdout:
            print("[*] Partições 'odate=*' encontradas no HDFS.")
            return True  # Retorna True se as partições forem encontradas
        else:
            print("[*] Nenhuma partição com 'odate=*' foi encontrada no HDFS.")
            return False  # Retorna False se não houver partições

    except subprocess.CalledProcessError as e:
        print(f"[*] Erro ao acessar o HDFS: {e.stderr}")
        return False  # Retorna False se ocorrer erro ao acessar o HDFS
    except Exception as e:
        print(f"[*] Ocorreu um erro inesperado: {str(e)}")
        return False  # Retorna False para outros erros



def processing_old_new(spark: SparkSession, df: DataFrame) -> DataFrame:

    schema = StructType([
        StructField("avatar", StringType(), True),
        StructField("id", StringType(), True),
        StructField("iso_date", StringType(), True),
        StructField("app", StringType(), False),
        StructField("segmento", StringType(), False),
        StructField("rating", StringType(), True),
        StructField("likes", LongType(), True),
        StructField("title", StringType(), True),
        StructField("snippet", StringType(), True),
        StructField("historical_data", ArrayType(StructType([
            StructField("title", StringType(), True),
            StructField("snippet", StringType(), True),
            StructField("app", StringType(), True),
            StructField("segmento", StringType(), True),
            StructField("rating", StringType(), True),
            StructField("iso_date", StringType(), True)
        ])), False),
        StructField("odate", StringType(), True),
    ])


    # Caminho para os dados históricos
    historical_data_path = "/santander/silver/compass/reviews/googlePlay/"  

    hdfs_path_exists = path_exists()

    if hdfs_path_exists:   

        # Obtenha a data atual
        current_date = datetime.now().strftime("%Y-%m-%d")
           

        df_historical = (
                    spark.read.schema(schema)
                    .parquet(f"{historical_data_path}/odate=*")
                    .withColumn("odate", to_date(col("odate"), "yyyyMMdd"))
                    .filter(col("odate") < lit(current_date))
                    .drop("odate")
                )
        
        new_reviews_df_alias = df.alias("new")
        historical_reviews_df_alias = df_historical.alias("old")

        # Junção dos DataFrames
        joined_reviews_df = new_reviews_df_alias.join(historical_reviews_df_alias, "id", "outer")

        # Criação da coluna historical_data
        result_df = joined_reviews_df.withColumn(
            "historical_data_temp",
            when(
                (col("new.title").isNotNull()) & (col("old.title").isNotNull()) & (col("new.title") != col("old.title")),
                F.array(
                    F.struct(
                        col("old.title").alias("title"),
                        col("old.snippet").alias("snippet"),
                        col("old.app").alias("app"),
                        col("new.segmento").alias("segmento"),
                        col("old.rating").cast("string").alias("rating"),
                        col("old.iso_date").alias("iso_date"),
                    )
                )
            ).when(
                (col("new.snippet").isNotNull()) & (col("old.snippet").isNotNull()) & (col("new.snippet") != col("old.snippet")),
                F.array(
                    F.struct(
                        col("old.title").alias("title"),
                        col("old.snippet").alias("snippet"),
                        col("old.app").alias("app"),
                        col("new.segmento").alias("segmento"),
                        col("old.rating").cast("string").alias("rating"),
                        col("old.iso_date").alias("iso_date"),
                    )
                )
            ).when(
                (col("new.rating").isNotNull()) & (col("old.rating").isNotNull()) & (col("new.rating") != col("old.rating")),
                F.array(
                    F.struct(
                        col("old.title").alias("title"),
                        col("old.snippet").alias("snippet"),
                        col("old.app").alias("app"),
                        col("new.segmento").alias("segmento"),
                        col("old.rating").cast("string").alias("rating"),
                        col("old.iso_date").alias("iso_date"),
                    )
                )
            ).when(
                (col("new.iso_date").isNotNull()) & (col("old.iso_date").isNotNull()) & (col("new.iso_date") != col("old.iso_date")),
                F.array(
                    F.struct(
                        col("old.title").alias("title"),
                        col("old.snippet").alias("snippet"),
                        col("old.app").alias("app"),
                        col("new.segmento").alias("segmento"),
                        col("old.rating").cast("string").alias("rating"),
                        col("old.iso_date").alias("iso_date"),
                    )
                )
            ).otherwise(
                F.array().cast("array<struct<title:string, snippet:string, app:string, segmento:string, rating:string, iso_date:string>>")
            )
        ).distinct()


        # Agrupando e coletando históricos
        df_final = result_df.groupBy("id").agg(
            F.coalesce(F.first("new.app"), F.first("old.app")).alias("app"),
            F.coalesce(F.first("new.rating"), F.first("old.rating")).alias("rating"),
            F.coalesce(F.first("new.iso_date"), F.first("old.iso_date")).alias("iso_date"),
            F.coalesce(F.first("new.title"), F.first("old.title")).alias("title"),
            F.coalesce(F.first("new.snippet"), F.first("old.snippet")).alias("snippet"),
            F.first("new.segmento").alias("segmento"),
            F.flatten(F.collect_list("historical_data_temp")).alias("historical_data")
        )

        return df_final

    else:
        print(f"[*] Caminho {historical_data_path} não existe no HDFS.")

        df_final = df.withColumn("historical_data", F.array().cast("array<struct<title:string, snippet:string, app:string, segmento:string, rating:string, iso_date:string>>"))

    return df_final



def save_metrics_job_fail(metrics_json):
    """
    Salva as métricas de aplicações com falhas
    """

    ES_HOST = "http://elasticsearch:9200"
    ES_INDEX = "compass_dt_datametrics_fail"
    ES_USER = os.environ["ES_USER"]
    ES_PASS = os.environ["ES_PASS"]

    # Conectar ao Elasticsearch
    es = Elasticsearch(
        [ES_HOST],
        basic_auth=(ES_USER, ES_PASS)
    )

    try:
        # Converter JSON em dicionário
        metrics_data = json.loads(metrics_json)

        # Inserir no Elasticsearch
        response = es.index(index=ES_INDEX, document=metrics_data)

        logging.info(f"[*] Métricas da aplicação salvas no Elasticsearch: {response}")
    except json.JSONDecodeError as e:
        logging.error(f"[*] Erro ao processar métricas: {e}", exc_info=True)
    except Exception as e:
        logging.error(f"[*] Erro ao salvar métricas no Elasticsearch: {e}", exc_info=True)
