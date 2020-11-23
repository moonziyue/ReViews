import os
from pyspark.sql import DataFrameWriter

POSTGRES_HOST = os.getenv('POSTGRES_HOST', '10.0.0.8')
POSTGRES_DB = os.getenv('POSTGRES_DB', 'amazon_reviews')
POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5431')
POSTGRES_USER = os.getenv('POSTGRES_USER')
POSTGRES_PWD = os.getenv('POSTGRES_PWD')
POSTGRES_URL = f'jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}'

ELASTIC_NODES = os.getenv('ELASTIC_HOST', '10.0.0.8')
ELASTIC_PORT = os.getenv('ELASTIC_PORT', '9200')

def write_to_postgres(df, table, mode):
	
    DataFrameWriter(df).jdbc(POSTGRES_URL, table, mode, {
        'user': POSTGRES_USER,
        'password': POSTGRES_PWD,
        'driver': 'org.postgresql.Driver'
    })

def write_to_es(df, index, mode):

	es_write_conf = {
        "es.nodes" : ELASTIC_NODES,
        "es.port" : ELASTIC_PORT,
        "es.resource" : index
    }

    df.write.format("org.elasticsearch.spark.sql").options(es_write_conf).mode(mode).save()