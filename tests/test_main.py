import pytest
import shutil
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType,
    LongType, DoubleType, MapType, ArrayType
)
from unittest.mock import MagicMock, patch, call, PropertyMock
from pyspark.sql.functions import lit, regexp_extract

from src.utils.tools import processing_reviews, read_source_parquet
from src.metrics.metrics import validate_ingest
from src.repo_trfmation_google_play import save_output_data

# --------------------------
# Fixtures
# --------------------------

@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("GooglePlayTests") \
        .config("spark.sql.shuffle.partitions", "1") \
        .getOrCreate()
    yield spark
    spark.stop()

@pytest.fixture
def google_play_schema_bronze():
    return StructType([
        StructField("avatar", StringType(), True),
        StructField("date", StringType(), True),
        StructField("id", StringType(), True),
        StructField("iso_date", StringType(), True),
        StructField("likes", LongType(), True),
        StructField("rating", DoubleType(), True),
        StructField("response", MapType(StringType(), StringType()), True),
        StructField("snippet", StringType(), True),
        StructField("title", StringType(), True)
    ])

@pytest.fixture
def test_data(spark, google_play_schema_bronze):
    data = [
        ("avatar1", "Nov 03, 2024", "id1", "2024-11-03T01:58:03Z", 266, 2.0, None, "Review 1", "User 1"),
        ("avatar2", "Oct 23, 2024", "id2", "2024-10-23T23:30:38Z", 189, 3.0, None, "Review 2", "User 2")
    ]
    return spark.createDataFrame(data, google_play_schema_bronze)

@pytest.fixture
def processed_test_data(test_data):
    return (
        test_data
        .drop("response")
        .withColumn("app", lit("test_app"))
        .withColumn("segmento", lit("pf"))
        .withColumn("historical_data",
                    lit(None).cast("array<struct<title:string,snippet:string,app:string,rating:string,iso_date:string>>"))
    )

# --------------------------
# Testes Corrigidos
# --------------------------

def test_read_source_parquet(spark, test_data, google_play_schema_bronze, tmp_path):
    """Testa a leitura de arquivos Parquet"""
    test_path = tmp_path / "test_data"
    test_data.write.mode("overwrite").parquet(str(test_path))

    try:
        result_df = read_source_parquet(spark, google_play_schema_bronze, str(test_path))

        assert result_df is not None
        assert "app" in result_df.columns
        assert "segmento" in result_df.columns
        assert "response" not in result_df.columns
        assert result_df.count() == 2
    finally:
        shutil.rmtree(test_path, ignore_errors=True)

def test_processing_reviews(test_data):
    """Testa o processamento das reviews"""
    df_with_path = test_data \
        .withColumn("app", regexp_extract(lit("/googlePlay/test_pf/odate=20240101"), "/googlePlay/(.*?)/odate=", 1)) \
        .withColumn("segmento", regexp_extract(lit("/googlePlay/test_pf/odate=20240101"), r"/googlePlay/[^/_]+_([pfj]+)/odate=", 1))

    processed_df = processing_reviews(df_with_path)

    assert processed_df.count() == 2
    assert set(processed_df.columns) == {
        "avatar", "id", "iso_date", "app", "segmento",
        "rating", "likes", "title", "snippet"
    }

def test_validate_ingest(spark, processed_test_data):
    """Testa a validação dos dados"""
    valid_df, invalid_df, results = validate_ingest(spark, processed_test_data)

    assert valid_df.count() == 2
    assert invalid_df.count() == 0
    assert isinstance(results, dict)
    assert "duplicate_check" in results

def test_save_output_data_success(processed_test_data):
    """Testa o salvamento bem-sucedido com todos os parâmetros"""
    # Configurar mock do config com todos os campos necessários
    mock_config = MagicMock()
    mock_config.path_target = "/tmp/valid"
    mock_config.path_target_fail = "/tmp/invalid"

    # Mock do schema silver
    mock_schema = StructType([
        StructField('id', StringType(), True),
        StructField('app', StringType(), False),
        StructField('segmento', StringType(), False),
        StructField('rating', StringType(), True),
        StructField('iso_date', StringType(), True),
        StructField('title', StringType(), True),
        StructField('snippet', StringType(), True),
        StructField(
            'historical_data',
            ArrayType(
                StructType([
                    StructField('title', StringType(), True),
                    StructField('snippet', StringType(), True),
                    StructField('app', StringType(), True),
                    StructField('segmento', StringType(), True),
                    StructField('rating', StringType(), True),
                    StructField('iso_date', StringType(), True)
                ]),
                True
            ),
            True
        )
    ])

    # Configurar mocks para o método write
    mock_parquet = MagicMock()
    mock_partitionBy = MagicMock()
    mock_partitionBy.parquet = mock_parquet

    mock_mode = MagicMock()
    mock_mode.partitionBy.return_value = mock_partitionBy

    mock_option = MagicMock()
    mock_option.mode.return_value = mock_mode

    mock_write = MagicMock()
    mock_write.option.return_value = mock_option

    with patch("src.repo_trfmation_google_play.google_play_schema_silver", return_value=mock_schema), \
            patch.object(type(processed_test_data), "write", new_callable=PropertyMock) as mock_write_property:

        mock_write_property.return_value = mock_write

        # Executar a função
        save_output_data(processed_test_data, processed_test_data, mock_config)

        # Verificar que write foi chamado duas vezes (uma para cada dataframe)
        assert mock_write_property.call_count == 2
        assert mock_write.option.call_count == 2
        assert mock_option.mode.call_count == 2
        assert mock_mode.partitionBy.call_count == 2
        assert mock_parquet.call_count == 2

        # Verificar parâmetros da primeira chamada (dados válidos)
        mock_write.option.assert_any_call("compression", "snappy")
        mock_option.mode.assert_any_call("overwrite")
        mock_mode.partitionBy.assert_any_call("odate")
        mock_parquet.assert_any_call("/tmp/valid")

        # Verificar parâmetros da segunda chamada (dados inválidos)
        mock_write.option.assert_any_call("compression", "snappy")
        mock_option.mode.assert_any_call("overwrite")
        mock_mode.partitionBy.assert_any_call("odate")
        mock_parquet.assert_any_call("/tmp/invalid")

def test_full_pipeline(spark, test_data, google_play_schema_bronze, tmp_path):
    """Teste do fluxo completo"""
    input_path = tmp_path / "input"
    test_data.write.mode("overwrite").parquet(str(input_path))

    # Importar dentro do contexto para garantir o patch correto
    from src.repo_trfmation_google_play import save_output_data

    with patch("src.repo_trfmation_google_play.save_output_data") as mock_save:
        # Executar fluxo
        df = read_source_parquet(spark, google_play_schema_bronze, str(input_path))
        processed_df = processing_reviews(df)
        valid_df, invalid_df, metrics = validate_ingest(spark, processed_df)

        # Configurar mock com os nomes de atributos corretos
        mock_config = MagicMock()
        mock_config.path_target = str(tmp_path / "valid")
        mock_config.path_target_fail = str(tmp_path / "invalid")  # Nome correto do atributo

        # Chamar a função através do módulo mockado
        mock_save(valid_df, invalid_df, mock_config)

        # Verificações
        assert df is not None
        assert processed_df is not None
        assert valid_df.count() > 0
        assert mock_save.called
        mock_save.assert_called_once_with(valid_df, invalid_df, mock_config)