import pytest
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, MapType
from datetime import datetime
from unittest.mock import MagicMock, patch
from pyspark.sql.functions import lit, regexp_extract, col

from src.utils.tools import processamento_reviews, read_source_parquet
from src.metrics.metrics import validate_ingest
from src.repo_trfmation_google_play import save_data

# Fixture do Spark para os testes
@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[1]") \
        .appName("GooglePlayTests") \
        .config("spark.sql.shuffle.partitions", "1") \
        .getOrCreate()

# Schema para testes
def google_play_schema_bronze():
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

# Fixture com dados de teste
@pytest.fixture
def test_data(spark):
    data = [
        ("avatar1", "Nov 03, 2024", "id1", "2024-11-03T01:58:03Z", 266, 2.0, None, "Review 1", "User 1"),
        ("avatar2", "Oct 23, 2024", "id2", "2024-10-23T23:30:38Z", 189, 3.0, None, "Review 2", "User 2"),
        ("avatar3", "Nov 14, 2024", "id3", "2024-11-14T14:01:17Z", 54, 1.0, None, "Review 3", "User 3")
    ]
    return spark.createDataFrame(data, google_play_schema_bronze())

def test_read_source_parquet(spark, test_data):
    """Testa a função de leitura de dados"""
    # Salvar dados de teste
    test_path = "/tmp/test_google_data"
    test_data.write.mode("overwrite").parquet(test_path)

    # Ler dados
    result_df = read_source_parquet(spark, google_play_schema_bronze(), test_path)

    # Verificações
    assert result_df is not None
    assert "app" in result_df.columns
    assert "segmento" in result_df.columns
    assert "response" not in result_df.columns

    # Limpar
    shutil.rmtree(test_path)

def test_processamento_reviews(spark, test_data):
    """Testa o processamento das reviews"""
    df_with_app = test_data.withColumn("app", regexp_extract(lit("googlePlay/test_pf/odate=20240101"), "/googlePlay/(.*?)/odate=", 1)) \
        .withColumn("segmento", regexp_extract(lit("googlePlay/test_pf/odate=20240101"), r"/googlePlay/[^/_]+_([pfj]+)/odate=", 1))

    # Processar dados
    processed_df = processamento_reviews(df_with_app)

    # Verificações
    assert processed_df.count() == df_with_app.count()
    assert "title" in processed_df.columns
    assert processed_df.first()["title"] == "USER 1"  # Verifica se foi convertido para maiúsculas
    assert "segmento" in processed_df.columns  # Verifica se segmento está presente

def test_validate_ingest(spark, test_data):
    """Testa a validação dos dados"""
    # Preparar dados como seria feito no fluxo normal
    df_with_app = test_data.withColumn("app", lit("test_app")) \
        .withColumn("segmento", lit("pf")) \
        .withColumn("historical_data", lit(None).cast("array<struct<title:string,snippet:string,app:string,rating:string,iso_date:string>>"))

    # Validar
    valid_df, invalid_df, results = validate_ingest(spark, df_with_app)

    # Verificações básicas
    assert valid_df.count() > 0
    assert isinstance(results, dict)
    assert "duplicate_check" in results
    assert "segmento" in valid_df.columns  # Verifica se segmento está presente

def test_save_data(spark, test_data):
    """Testa o salvamento dos dados"""
    # Criar DataFrame com todas as colunas necessárias
    df_with_all_columns = test_data.withColumn("app", lit("test_app")) \
        .withColumn("segmento", lit("pf")) \
        .withColumn("historical_data", lit(None).cast("array<struct<title:string,snippet:string,app:string,rating:string,iso_date:string>>"))

    # Mock para teste
    with patch("pyspark.sql.DataFrameWriter.parquet") as mock_parquet:
        # Chamando a função a ser testada
        save_data(df_with_all_columns, df_with_all_columns, "/tmp/valid", "/tmp/invalid")

        # Verificando se o método parquet foi chamado
        assert mock_parquet.call_count == 2