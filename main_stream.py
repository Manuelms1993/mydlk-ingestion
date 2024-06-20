import os

from pyspark.sql import SparkSession

# Initialize Spark session with Hive support
from batch.sources.fileSourceToHive import FileSource
from streaming.sources.kafkaSource import KafkaSource
from streaming.utils import createHiveStreamingTables

if __name__ == "__main__":

    spark = SparkSession.builder \
        .appName("Ingestion") \
        .appName("Spark with Hive and Hadoop") \
        .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
        .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1") \
        .config("dfs.datanode.use.datanode.hostname", "true") \
        .config("dfs.client.use.datanode.hostname", "true") \
        .enableHiveSupport() \
        .getOrCreate()

    # process
    source = KafkaSource()
    source.createStructure(spark)
    source.ingestion(spark)


