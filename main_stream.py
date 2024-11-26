from datetime import datetime
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, StructField, IntegerType
from pyspark.sql import SparkSession
from db.DB import streamdb
from db.tables import create_stream_actors


def foreach_batch_function(batch_df, batch_id):
    batch_df.write \
        .mode("append") \
        .insertInto("streaming.actors")
    print("Data inserted " + str(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

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
    spark.sql(streamdb)
    # spark.sql("DROP TABLE IF EXISTS streaming.actors")
    spark.sql(create_stream_actors)

    schema = StructType([
        StructField("person_id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("characterType", StringType(), True),
        StructField("role", StringType(), True),
        StructField("timeMark", StringType(), True)
    ])

    stream = spark.readStream.format("kafka") \
        .option("subscribe", "actors_topic") \
        .option("kafka.bootstrap.servers", "127.0.0.1:9092").load()

    query = stream \
        .select(from_json(col("value").cast("string"), schema).alias("data")) \
        .select("data.*") \
        .writeStream \
        .foreachBatch(foreach_batch_function) \
        .trigger(processingTime='15 seconds') \
        .start()

    query.awaitTermination()


