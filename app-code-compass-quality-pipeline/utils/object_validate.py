from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, ArrayType, LongType,
    IntegerType, BooleanType, FloatType, TimestampType, DateType, MapType
)


###########################################################################################################
# Validação: Schema bronze
###########################################################################################################
# Esquema para Google Play
def google_play_schema_bronze():
    return StructType([
        StructField('avatar', StringType(), True),
        StructField('date', StringType(), True),
        StructField('id', StringType(), True),
        StructField('iso_date', StringType(), True),
        StructField('likes', LongType(), True),
        StructField('rating', DoubleType(), True),
        StructField('snippet', StringType(), True),
        StructField('title', StringType(), True)
    ])


# Esquema para MongoDB
def mongodb_schema_bronze():
    return StructType([
        StructField('id', StringType(), True),
        StructField('comment', StringType(), True),
        StructField('votes_count', IntegerType(), True),
        StructField('os', StringType(), True),
        StructField('os_version', StringType(), True),
        StructField('country', StringType(), True),
        StructField('age', IntegerType(), True),
        StructField('customer_id', StringType(), True), # mudar para string
        StructField('cpf', StringType(), True),
        StructField('app_version', StringType(), True),
        StructField('rating', IntegerType(), True),
        StructField('timestamp', StringType(), True),
        StructField('app', StringType(), True)
    ])

def apple_store_schema_bronze():
    return StructType([
        StructField('author_name', StringType(), True),
        StructField('author_uri', StringType(), True),
        StructField('content', StringType(), True),
        StructField('content_attributes_label', StringType(), True),
        StructField('content_attributes_term', StringType(), True),
        StructField('id', StringType(), True),
        StructField('im_rating', StringType(), True),
        StructField('im_version', StringType(), True),
        StructField('im_votecount', StringType(), True),
        StructField('im_votesum', StringType(), True),
        StructField('link_attributes_href', StringType(), True),
        StructField('link_attributes_related', StringType(), True),
        StructField('title', StringType(), True),
        StructField('updated', StringType(), True)
    ])



###########################################################################################################
# Validação: Schema SILVER
###########################################################################################################
# Esquema para Google Play
def google_play_schema_silver():
    return StructType([
        StructField("id", StringType(), True),
        StructField("app", StringType(), True),
        StructField("rating", DoubleType(), True),
        StructField("iso_date", StringType(), True),
        StructField("title", StringType(), True),
        StructField("snippet", StringType(), True),
        StructField("historical_data", ArrayType(
                StructType([
                    StructField("title", StringType(), True),
                    StructField("snippet", StringType(), True),
                    StructField("app", StringType(), True),
                    StructField("rating", StringType(), True),
                    StructField("iso_date", StringType(), True)
                ]),
                True
            ),
            True
        )
    ])

# Esquema para MongoDB
def mongodb_schema_silver():
    return StructType([
        StructField('id', StringType(), True),
        StructField('customer_id', StringType(), True),
        StructField('cpf', StringType(), True),
        StructField('app', StringType(), True),
        StructField('rating', StringType(), True),
        StructField('timestamp', StringType(), True),
        StructField('comment', StringType(), True),
        StructField('app_version', StringType(), True),
        StructField('os_version', StringType(), True),
        StructField('os', StringType(), True),
        StructField('historical_data', ArrayType(ArrayType(StructType([
            StructField('customer_id', StringType(), True),
            StructField('cpf', StringType(), True),
            StructField('app', StringType(), True),
            StructField('comment', StringType(), True),
            StructField('rating', StringType(), True),
            StructField('timestamp', StringType(), True),
            StructField('app_version', StringType(), True),
            StructField('os_version', StringType(), True),
            StructField('os', StringType(), True)
        ]), True), True), True)
    ])

def apple_store_schema_silver():
    return StructType([
        StructField("id", StringType(), True),
        StructField("name_client", StringType(), True),
        StructField("app", StringType(), True),
        StructField("im_version", StringType(), True),
        StructField("im_rating", StringType(), True),
        StructField("title", StringType(), True),
        StructField("content", StringType(), True),
        StructField("updated", StringType(), True),
        StructField("historical_data", ArrayType(
            ArrayType(StructType([
                StructField("title", StringType(), True),
                StructField("content", StringType(), True),
                StructField("app", StringType(), True),
                StructField("im_version", StringType(), True),
                StructField("im_rating", StringType(), True)
            ]), True), True))
    ])



