
import pytest
import shutil
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, MapType, ArrayType
from datetime import datetime
from unittest.mock import MagicMock, patch
from src.metrics.metrics import validate_ingest
from src.utils.tools import read_data, processamento_reviews
from src.repo_trfmation_google_play import save_data

@pytest.fixture(scope="session")
def spark():
    """
    Fixture que inicializa o SparkSession para os testes.
    """
    spark = SparkSession.builder.master("local").appName("TestApp").getOrCreate()
    yield spark
    spark.stop()


# Definir o esquema para os dados
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



# Configuração do pytest para o Spark
@pytest.fixture(scope="session")
def spark():
    # Inicializar uma SparkSession para os testes
    return SparkSession.builder.master("local[1]").appName("TestReadData").getOrCreate()

def data_google():
    return [
        ("https://play-lh.googleusercontent.com/a/ACg8ocKVR7jALSxc3SZfSeBv5ZysOFoXIkngoE0O_J5HfUql9W836w=mo", "November 03, 2024", "c484bb1e-eebb-4609-93c4-d21d00f648ed", "2024-11-03T01:58:03Z", 266, 2.0, None, "As notificações do app não funciona depois da última atualização...", "Robson Santos"),
        ("https://play-lh.googleusercontent.com/a-/ALV-UjU0-K2FzgFHxvvsqYDDjM5hdBFKxogfpXOI30yUSGPd2jrwYyUlzA", "October 23, 2024", "1ccda588-f60f-4b4d-b9a2-00234e903628", "2024-10-23T23:30:38Z", 189, 3.0, None, "App leve, rápido, fácil de navegar. Gostei muito. Só sinto falta de...", "Carlos Alberto Souza"),
        ("https://play-lh.googleusercontent.com/a-/ALV-UjXz3IdoGmdiJywZYfufnWNGoC041A8vtvb4SLgclRKicixck7A7Cg", "November 14, 2024", "9b0fb626-a84b-4a19-86a9-c3d7e6d00138", "2024-11-14T14:01:17Z", 54, 1.0, None, "O aplicativo Santander apresenta falhas constantes e exige confirmação...", "Tanios Toledo"),
        ("https://play-lh.googleusercontent.com/a-/ALV-UjWqUa0ZCAcbIpqRM9uo9604gjuDyLg2c5mR-9LLgJLesdBEZ00ooQ", "October 01, 2024", "69a3d3db-7760-47d0-9fc1-a9cb058c8402", "2024-10-01T21:33:15Z", 136, 2.0, None, "Interface deixa a muito a indesejar e umas complicações chatas...", "Rafa GP"),
        ("https://play-lh.googleusercontent.com/a-/ALV-UjWqUa0ZCAcbIpqRM9uo9604gjuDyLg2c5mR-9LLgJLesdBEZ00ooQ", "October 01, 2024", "69a3d3db-7760-47d0-9fc1-a9cb058c8402", "2024-10-01T21:33:15Z", 136, 2.0, None, "Interface deixa a muito a indesejar e umas complicações chatas...", "Rafa GP"),
        ("https://play-lh.googleusercontent.com/a-/ALV-UjWqUa0ZCAcbIpqRM9uo9604gjuDyLg2c5mR-9LLgJLesdBEZ00ooQ", "October 01, 2024", "69a3d3db-7760-47d0-9fc1-a9cb058c8402", "2024-10-01T21:33:15Z", 136, 2.0, None, "Interface deixa a muito a indesejar e umas complicações chatas...", "Rafa GP"),
        ("https://play-lh.googleusercontent.com/a-/ALV-UjXYIpyQqCUqTFoywYt5PSMGPSgPYT-GqlXM5m9SStORtNyBLoK2eGQ", "October 12, 2024", "8a20d295-fdb8-4ab6-84e1-c5e3eb1f2578", "2024-10-12T14:10:56Z", 95, 4.0, None, "A interface está cada vez melhor. O app do Santander está no caminho certo...", "Pedro Henrique Melo"),
        ("https://play-lh.googleusercontent.com/a-/ALV-UjURFIlCwxWzLfqZc-5aNyQRGgfXYPOG08SDYtZ1HO7oISjUsQn87g", "November 10, 2024", "ad17328f-784d-4e63-bdf9-309bdb3781eb", "2024-11-10T10:00:45Z", 84, 3.0, None, "O aplicativo em geral é bom, mas sinto falta de integração com dispositivos...", "Lucia Alves"),
    ]


# Teste unitário para a função read_data
def test_read_data(spark):
    # Criando um DataFrame de teste com dados fictícios
    test_data = data_google()

    schema = google_play_schema_bronze()
    # Criar o DataFrame com os dados de teste
    df_test = spark.createDataFrame(test_data, schema)

    # Salvar o DataFrame como um arquivo Parquet temporário
    test_parquet_path = "/tmp/test_google_play_data"
    df_test.write.mode("overwrite").parquet(test_parquet_path)

    # Chamar a função que você está testando
    result_df = read_data(spark, schema, test_parquet_path)

    # Verifique se a coluna "app" foi criada corretamente
    assert "app" in result_df.columns, "A coluna 'app' não foi criada corretamente."

    # Verifique se a coluna "response" foi removida
    assert "response" not in result_df.columns, "A coluna 'response' não foi removida."

    # Verifique se o número de registros no DataFrame é o esperado
    assert result_df.count() == 8, f"Esperado 8 registros, mas encontrou {result_df.count()}."

    # Limpar o arquivo de teste após o teste (opcional)
    shutil.rmtree(test_parquet_path)

def test_processamento_reviews(spark):
    # Criando um DataFrame de teste com dados fictícios
    test_data = data_google()

    schema = google_play_schema_bronze()
    # Criar o DataFrame com os dados de teste
    df_test = spark.createDataFrame(test_data, schema)

    # Salvar o DataFrame como um arquivo Parquet temporário
    test_parquet_path = "/tmp/test_google_play_data"
    df_test.write.mode("overwrite").parquet(test_parquet_path)

    # Chamar a função que você está testando
    result_df = read_data(spark, schema, test_parquet_path)

    # Processamento dos dados
    df_processado = processamento_reviews(result_df)

    assert df_processado.count() > 0, "[*] Dataframe vazio!"

def test_validate_ingest(spark):
    """
    Testa a função de validação de ingestão para garantir que os DataFrames têm dados e que a validação gera resultados.
    """
    # Criando um DataFrame de teste com dados fictícios
    test_data = data_google()

    schema = google_play_schema_bronze()
    # Criar o DataFrame com os dados de teste
    df_test = spark.createDataFrame(test_data, schema)

    # Salvar o DataFrame como um arquivo Parquet temporário
    test_parquet_path = "/tmp/test_google_play_data"
    df_test.write.mode("overwrite").parquet(test_parquet_path)

    # Chamar a função que você está testando
    result_df = read_data(spark, schema, test_parquet_path)

    # Processamento dos dados
    df_processado = processamento_reviews(result_df)


    # Valida o DataFrame e coleta resultados
    valid_df, invalid_df, validation_results = validate_ingest(spark, df_processado)

    assert valid_df.count() > 0, "[*] O DataFrame válido está vazio!"
    assert invalid_df.count() > 0, "[*] O DataFrame inválido está vazio!"
    assert len(validation_results) > 0, "[*] Não foram encontrados resultados de validação!"

    # Opcional: Exibir resultados para depuração
    print("Testes realizados com sucesso!")
    print(f"Total de registros válidos: {valid_df.count()}")
    print(f"Total de registros inválidos: {invalid_df.count()}")
    print(f"Resultados da validação: {validation_results}")

def test_save_data():
    # Configurando SparkSession para testes
    spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()

    # Criando um DataFrame de teste com dados fictícios
    test_data = data_google()

    schema = google_play_schema_bronze()
    # Criar o DataFrame com os dados de teste
    df_test = spark.createDataFrame(test_data, schema)

    # Salvar o DataFrame como um arquivo Parquet temporário
    test_parquet_path = "/tmp/test_google_play_data"
    df_test.write.mode("overwrite").parquet(test_parquet_path)

    # Chamar a função que você está testando
    result_df = read_data(spark, schema, test_parquet_path)

    # Processamento dos dados
    df_processado = processamento_reviews(result_df)


    # Valida o DataFrame e coleta resultados
    valid_df, invalid_df, validation_results = validate_ingest(spark, df_processado)

    # Definindo caminhos
    datePath = datetime.now().strftime("%Y%m%d")
    path_target = f"/tmp/fake/path/valid/odate={datePath}/"
    path_target_fail = f"/tmp/fake/path/invalid/odate={datePath}/"

    # Mockando o método parquet
    with patch("pyspark.sql.DataFrameWriter.parquet", MagicMock()) as mock_parquet:
        # Chamando a função a ser testada
        save_data(valid_df, invalid_df, path_target, path_target_fail)

        # Verificando se o método parquet foi chamado com os caminhos corretos
        mock_parquet.assert_any_call(path_target)
        mock_parquet.assert_any_call(path_target_fail)

    print("[*] Teste de salvar dados concluído com sucesso!")