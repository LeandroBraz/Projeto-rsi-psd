from __future__ import print_function

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import split
from pyspark.sql.functions import approxCountDistinct
import requests

THINGSBOARD_HOST = '127.0.0.1'
THINGSBOARD_PORT = '9090'
ACCESS_TOKEN = 'HTpbdoIjtFRPgVcbJb1f'
url = 'http://' + THINGSBOARD_HOST + ':' + THINGSBOARD_PORT + '/api/v1/' + ACCESS_TOKEN + '/telemetry'
headers = {}
headers['Content-Type'] = 'application/json'

def processRow(row):
    print(row)
    row_data = { row.word : row.__getitem__("count")}
    requests.post(url, json=row_data)

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("""
        Usage: structured_kafka_wordcount.py <bootstrap-servers> <subscribe-type> <topics>
        """, file=sys.stderr)
        sys.exit(-1)

    bootstrapServers = sys.argv[1]
    subscribeType = sys.argv[2]
    topics = sys.argv[3]

    spark = SparkSession\
        .builder\
        .appName("consumidor")\
        .getOrCreate()

    # Create DataSet representing the stream of input lines from kafka
    lines = spark\
        .readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", bootstrapServers)\
        .option(subscribeType, topics)\
        .load()\
        .selectExpr("CAST(value AS STRING)")

    # Cria a tabela de dados
    dados = lines.select(
        split(lines.value, ', ')[0].alias("Source"),
        split(lines.value, ', ')[1].alias("Time"),
        split(lines.value, ', ')[2].alias("ssid"),
        split(lines.value, ', ')[3].alias("marca")
    )

    #conta a quantidade de devices diferentes
    
    qtdDeviceDif = dados.agg(approxCountDistinct('Source'))
    query = qtdDeviceDif\
        .writeStream\
        .outputMode('complete')\
        .format('console')\
        .start()

    query.awaitTermination()
    
    #conta a quantidade de devices diferentes
    
    qtdDeviceDif = dados.agg(approxCountDistinct('Source'))
    query = qtdDeviceDif\
        .writeStream\
        .outputMode('complete')\
        .foreach(processRow)\
        .start()

    query.awaitTermination()
    

    