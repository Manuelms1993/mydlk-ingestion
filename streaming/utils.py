from pyspark.sql import SparkSession

from db.DB import streamdb
from db.tables import create_stream_actors

def createHiveStreamingTables():

    spark = SparkSession.builder \
        .appName("Ingestion") \
        .appName("Spark with Hive and Hadoop") \
        .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
        .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
        .config("dfs.datanode.use.datanode.hostname", "true") \
        .config("dfs.client.use.datanode.hostname", "true") \
        .enableHiveSupport() \
        .getOrCreate()

    spark.sql(streamdb)
    spark.sql("DROP TABLE IF EXISTS raw.films")
    spark.sql(create_stream_actors)
    spark.stop()