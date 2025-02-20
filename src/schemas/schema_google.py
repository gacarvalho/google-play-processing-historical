from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, DoubleType

def google_play_schema_silver():
    return StructType([
        StructField('id', StringType(), True),
        StructField('app', StringType(), False),
        StructField('segmento', StringType(), False),
        StructField('rating', DoubleType(), True),
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
