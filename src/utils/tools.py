"""
Módulo com funções utilitárias para processamento de dados no pipeline de reviews da Google Play.

Funções principais:
- read_source_parquet: Leitura de arquivos Parquet com tratamento de erros
- processing_reviews: Processamento básico dos dados de reviews
- save_dataframe: Salvamento robusto de DataFrames
- save_metrics: Integração com Elasticsearch para métricas
- processing_old_new: Comparação entre dados novos e históricos
"""

import os
import subprocess
import json
import logging
import traceback
from datetime import datetime
from pathlib import Path
from typing import Optional, Union
from urllib.parse import quote_plus

import pyspark.sql.functions as F
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, to_date, when, input_file_name,
    regexp_extract, coalesce, collect_list, struct, array, broadcast, rand, floor
)
from pyspark.sql.utils import AnalysisException
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, LongType, ArrayType, IntegerType, MapType
)
from unidecode import unidecode
from elasticsearch import Elasticsearch

# Configuração básica de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Importação flexível do schema
try:
    from schema_google import google_play_schema_silver
except ImportError:
    from src.schemas.schema_google import google_play_schema_silver

def remove_accents(s: str) -> str:
    """Remove acentos e caracteres especiais de uma string"""
    return unidecode(s)

remove_accents_udf = F.udf(remove_accents, StringType())

def read_source_parquet(
        spark: SparkSession,
        schema: StructType,
        path: str
    ) -> Optional[DataFrame]:
    """
    Tenta ler um arquivo Parquet e retorna None se não houver dados.

    Args:
        spark: Sessão do Spark
        schema: Schema esperado dos dados
        path: Caminho do arquivo Parquet

    Returns:
        DataFrame com os dados ou None se falhar
    """
    try:
        df = spark.read.schema(schema).parquet(path)
        if df.isEmpty():
            logger.error(f"[*] Nenhum dado encontrado em: {path}")
            return None

        return (df
                .withColumn("app", regexp_extract(input_file_name(), "/googlePlay/(.*?)/odate=", 1))
                .drop("response")
                .withColumn("segmento", regexp_extract(input_file_name(), r"/googlePlay/[^/_]+_([pfj]+)/odate=", 1)))

    except AnalysisException as e:
        logger.error(f"[*] Falha ao ler: {path}. Erro: {str(e)}")
        return None
    except Exception as e:
        logger.error(f"[*] Erro inesperado ao ler {path}: {str(e)}")
        return None

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


def processing_reviews(df: DataFrame) -> DataFrame:
    """
    Processa o DataFrame de reviews aplicando transformações básicas.

    Args:
        df: DataFrame com os dados brutos de reviews

    Returns:
        DataFrame processado
    """
    logger.info(f"{datetime.now().strftime('%Y%m%d %H:%M:%S.%f')} [*] Processando reviews da Google Play")

    return df.select(
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

def get_schema(df: DataFrame, schema: StructType) -> DataFrame:
    """
    Ajusta o DataFrame para seguir o schema especificado.

    Args:
        df: DataFrame a ser ajustado
        schema: Schema desejado

    Returns:
        DataFrame com schema aplicado
    """
    for field in schema.fields:
        if field.dataType == IntegerType():
            df = df.withColumn(field.name, df[field.name].cast(IntegerType()))
        elif field.dataType == StringType():
            df = df.withColumn(field.name, df[field.name].cast(StringType()))
    return df.select([field.name for field in schema.fields])

def save_dataframe(
        df: DataFrame,
        path: str,
        label: str,
        schema: Optional[StructType] = None,
        partition_column: str = "odate",
        compression: str = "snappy"
    ) -> bool:
    """
    Salva um DataFrame Spark no formato Parquet de forma robusta.

    Args:
        df: DataFrame a ser salvo
        path: Caminho de destino
        label: Identificação para logs (ex: 'valido', 'invalido')
        schema: Schema opcional para validação
        partition_column: Coluna de partição
        compression: Tipo de compressão

    Returns:
        bool: True se salvou com sucesso, False caso contrário

    Raises:
        ValueError: Se os parâmetros forem inválidos
        IOError: Se houver problemas ao escrever no filesystem
    """
    if not isinstance(df, DataFrame):
        logger.error(f"[*] Objeto passado não é um DataFrame Spark: {type(df)}")
        return False

    if not path:
        logger.error("Caminho de destino não pode ser vazio")
        return False

    current_date = datetime.now().strftime('%Y%m%d')
    full_path = Path(path)

    try:
        if schema:
            logger.info(f"[*] Aplicando schema para dados {label}")
            df = get_schema(df, schema)

        df_partition = df.withColumn(partition_column, lit(current_date))

        if not df_partition.head(1):
            logger.warning(f"[*] Nenhum dado {label} encontrado para salvar")
            return False

        try:
            full_path.mkdir(parents=True, exist_ok=True)
            logger.debug(f"[*] Diretório {full_path} verificado/criado")
        except Exception as dir_error:
            logger.error(f"[*] Falha ao preparar diretório {full_path}: {dir_error}")
            raise IOError(f"[*] Erro de diretório: {dir_error}") from dir_error

        logger.info(f"[*] Salvando {df_partition.count()} registros ({label}) em {full_path}")

        (df_partition.write
         .option("compression", compression)
         .mode("overwrite")
         .partitionBy(partition_column)
         .parquet(str(full_path)))

        logger.info(f"[*] Dados {label} salvos com sucesso em {full_path}")
        return True

    except Exception as e:
        error_msg = f"[*] Falha ao salvar dados {label} em {full_path}"
        logger.error(error_msg, exc_info=True)
        logger.error(f"[*] Detalhes do erro: {str(e)}\n{traceback.format_exc()}")
        return False

def save_metrics(
        metrics_type: str,
        index: str,
        error: Optional[Exception] = None,
        df: Optional[DataFrame] = None,
        metrics_data: Optional[Union[dict, str]] = None
) -> None:
    """
    Salva métricas no Elasticsearch com estruturas específicas.

    Args:
        metrics_type: 'success' ou 'fail'
        index: Nome do índice no Elasticsearch
        error: Objeto de exceção (para tipo 'fail')
        df: DataFrame (para extrair segmentos)
        metrics_data: Dados das métricas (para tipo 'success')

    Raises:
        ValueError: Se os parâmetros forem inválidos
    """
    metrics_type = metrics_type.lower()

    if metrics_type not in ('success', 'fail'):
        raise ValueError("[*] O tipo deve ser 'success' ou 'fail'")

    if metrics_type == 'fail' and not error:
        raise ValueError("[*] Para tipo 'fail', o parâmetro 'error' é obrigatório")

    if metrics_type == 'success' and not metrics_data:
        raise ValueError("[*] Para tipo 'success', 'metrics_data' é obrigatório")

    ES_HOST = os.getenv("ES_HOST", "http://elasticsearch:9200")
    ES_USER = os.getenv("ES_USER")
    ES_PASS = os.getenv("ES_PASS")

    if not all([ES_USER, ES_PASS]):
        raise ValueError("[*] Credenciais do Elasticsearch não configuradas")

    if metrics_type == 'fail':
        try:
            segmentos_unicos = [row["segmento"] for row in df.select("segmento").distinct().collect()] if df else ["UNKNOWN_CLIENT"]
        except Exception:
            logger.warning("[*] Não foi possível extrair segmentos. Usando 'UNKNOWN_CLIENT'.")
            segmentos_unicos = ["UNKNOWN_CLIENT"]

        document = {
            "timestamp": datetime.now().isoformat(),
            "layer": "silver",
            "project": "compass",
            "job": "google_play_reviews",
            "priority": "0",
            "tower": "SBBR_COMPASS",
            "client": segmentos_unicos,
            "error": str(error) if error else "Erro desconhecido"
        }
    else:
        if isinstance(metrics_data, str):
            try:
                document = json.loads(metrics_data)
            except json.JSONDecodeError as e:
                raise ValueError("[*] metrics_data não é um JSON válido") from e
        else:
            document = metrics_data

    try:
        es = Elasticsearch(
            hosts=[ES_HOST],
            basic_auth=(ES_USER, ES_PASS),
            request_timeout=30
        )

        response = es.index(
            index=index,
            document=document
        )

        logger.info(f"[*] Métricas salvas com sucesso no índice {index}. ID: {response['_id']}")
        return response

    except Exception as es_error:
        logger.error(f"[*] Falha ao salvar no Elasticsearch: {str(es_error)}")
        raise
    except Exception as e:
        logger.error(f"[*] Erro inesperado: {str(e)}")
        raise

def path_exists() -> bool:
    """
    Verifica se o caminho de dados históricos existe no HDFS.

    Returns:
        bool: True se existir, False caso contrário
    """
    historical_data_path = "/santander/silver/compass/reviews/googlePlay/"

    try:
        # Verificação inicial de existência do caminho
        if os.system(f"hadoop fs -test -e {historical_data_path}") != 0:
            logger.warning(f"[*] O caminho {historical_data_path} não existe no HDFS.")
            return False

        # Verificação mais detalhada das partições
        cmd = f"hdfs dfs -ls {historical_data_path}"
        result = subprocess.run(
            cmd.split(),
            capture_output=True,
            text=True,
            check=True
        )

        if "odate=" in result.stdout:
            logger.info("[*] Partições 'odate=*' encontradas no HDFS.")
            return True

        logger.info("[*] Nenhuma partição com 'odate=*' foi encontrada no HDFS.")
        return False

    except subprocess.CalledProcessError as e:
        logger.error(f"[*] Erro ao acessar o HDFS: {e.stderr}")
        return False
    except Exception as e:
        logger.error(f"[*] Ocorreu um erro inesperado: {str(e)}")
        return False

def processing_old_new(spark: SparkSession, df: DataFrame) -> DataFrame:
    """
    Compara dados novos com históricos, gerando registro de alterações.

    Args:
        spark: Sessão Spark
        df: DataFrame com dados novos

    Returns:
        DataFrame com dados combinados e histórico de mudanças
    """
    current_date = datetime.now().strftime("%Y-%m-%d")
    historical_data_path = "/santander/silver/compass/reviews/googlePlay/"

    # Definindo schema para o histórico
    historical_schema = StructType([
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

    if path_exists():
        df_historical = (
            spark.read.schema(historical_schema)
            .option("basePath", historical_data_path)
            .parquet(f"{historical_data_path}/odate=*")
            .withColumn("odate", to_date(col("odate"), "yyyyMMdd"))
            .filter(col("odate") < lit(current_date))
            .drop("odate")
        )
    else:
        logger.warning(f"[*] Caminho {historical_data_path} não existe no HDFS.")
        df_historical = spark.createDataFrame([], historical_schema)

    # Aliases para os DataFrames
    # new_df = df.alias("new")
    # old_df = df_historical.alias("old")

    # Junção e processamento dos dados
    # joined_df = new_df.join(old_df, "id", "outer")

    """
    Aplicamos Salting que é técnica para evitar skew (desequilíbrio de diff de dados (files=particoes) no join.
    
        -> Adicionamos uma coluna extra com valores aleatórios (salt) para dividir a chave principal, distribuindo melhor os dados entre as tasks do Spark.
        -> Por exemplo: se a chave do join for 'id' e adicionarmos um 'salt' de 0 a 9, o join passa a ser feito por ('id', 'salt') em vez de apenas 'id'.
    
        Isso distribui melhor os dados entre as tasks do Spark, evitando sobrecarga em uma única partição.
    
    Após o join, a coluna 'salt' é removida para manter a integridade dos dados.
    """

    max_broadcast_size_mb = 50

    # Estimativa simples em bytes do histórico
    size_in_bytes = df_historical.rdd.map(lambda r: len(str(r))).sum()
    size_in_mb = size_in_bytes / (1024 * 1024)

    if size_in_mb <= max_broadcast_size_mb:
        # Se o DataFrame histórico for pequeno (até 64MB), usamos broadcast join.
        # O Spark envia o histórico para todos os executores, evitando shuffle.
        # Isso melhora a performance e evita problemas com partições desbalanceadas,
        # onde os dados antigos são leves e os dados novos são muito pesados.
        joined_reviews_df = df.alias("new").join(broadcast(df_historical).alias("old"), "id", "outer")
    else:
        # Se o histórico for grande, usamos salting para evitar skew no join.
        # Adicionamos uma coluna 'salt' com valores aleatórios de 0 a 9 em ambos os DataFrames.
        # O join é feito por (id, salt), espalhando a carga entre várias partições.
        # Isso distribui melhor os dados, evitando que uma única chave sobrecarregue uma task.
        salt_count = 10
        df_salted = df.withColumn("salt", (floor(rand() * salt_count)).cast("int"))
        df_hist_salted = df_historical.withColumn("salt", (floor(rand() * salt_count)).cast("int"))

        joined_reviews_df = df_salted.alias("new").join(df_hist_salted.alias("old"),(col("new.id") == col("old.id")) & (col("new.salt") == col("old.salt")),"outer").drop("new.salt", "old.salt")

    result_df = joined_reviews_df.withColumn(
        "historical_data_temp",
        when(
            (col("new.title").isNotNull()) &
            (col("old.title").isNotNull()) &
            (col("new.title") != col("old.title")),
            array(
                struct(
                    col("old.title").alias("title"),
                    col("old.snippet").alias("snippet"),
                    col("old.app").alias("app"),
                    col("new.segmento").alias("segmento"),
                    col("old.rating").cast("string").alias("rating"),
                    col("old.iso_date").alias("iso_date")
                )
            )
        ).when(
            (col("new.snippet").isNotNull()) &
            (col("old.snippet").isNotNull()) &
            (col("new.snippet") != col("old.snippet")),
            array(
                struct(
                    col("old.title").alias("title"),
                    col("old.snippet").alias("snippet"),
                    col("old.app").alias("app"),
                    col("new.segmento").alias("segmento"),
                    col("old.rating").cast("string").alias("rating"),
                    col("old.iso_date").alias("iso_date")
                )
            )
        ).when(
            (col("new.rating").isNotNull()) &
            (col("old.rating").isNotNull()) &
            (col("new.rating") != col("old.rating")),
            array(
                struct(
                    col("old.title").alias("title"),
                    col("old.snippet").alias("snippet"),
                    col("old.app").alias("app"),
                    col("new.segmento").alias("segmento"),
                    col("old.rating").cast("string").alias("rating"),
                    col("old.iso_date").alias("iso_date")
                )
            )
        ).when(
            (col("new.iso_date").isNotNull()) &
            (col("old.iso_date").isNotNull()) &
            (col("new.iso_date") != col("old.iso_date")),
            array(
                struct(
                    col("old.title").alias("title"),
                    col("old.snippet").alias("snippet"),
                    col("old.app").alias("app"),
                    col("new.segmento").alias("segmento"),
                    col("old.rating").cast("string").alias("rating"),
                    col("old.iso_date").alias("iso_date")
                )
            )
        ).otherwise(
            array().cast("array<struct<title:string, snippet:string, app:string, segmento:string, rating:string, iso_date:string>>")
        )
    ).distinct()

    # Agrupamento final
    df_final = result_df.groupBy("id").agg(
        coalesce(F.first("new.avatar"), F.first("old.avatar")).alias("avatar"),
        coalesce(F.first("new.app"), F.first("old.app")).alias("app"),
        coalesce(F.first("new.rating"), F.first("old.rating")).alias("rating"),
        coalesce(F.first("new.iso_date"), F.first("old.iso_date")).alias("iso_date"),
        coalesce(F.first("new.title"), F.first("old.title")).alias("title"),
        coalesce(F.first("new.snippet"), F.first("old.snippet")).alias("snippet"),
        F.first("new.segmento").alias("segmento"),
        F.first("new.likes").alias("likes"),
        F.flatten(collect_list("historical_data_temp")).alias("historical_data")
    )

    logger.info("[*] Processamento de dados históricos concluído")
    return df_final