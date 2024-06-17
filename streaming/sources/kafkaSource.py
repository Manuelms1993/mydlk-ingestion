
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, StructField, IntegerType

from streaming.sources.source import Source

class KafkaSource(Source):

    def ingestion(self, spark: SparkSession):

        schema = StructType([
            StructField("person_id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("characterType", StringType(), True),
            StructField("role", StringType(), True),
            StructField("timeMark", StringType(), True)
        ])

        def foreach_batch_function(batch_df, batch_id):
            batch_df.write \
                .mode("append") \
                .insertInto("streaming.actors")

        stream = spark.readStream.format("kafka") \
            .option("subscribe", "actors_topic") \
            .option("kafka.bootstrap.servers", "127.0.0.1:9092").load()

        query = stream\
            .select(from_json(col("value").cast("string"), schema).alias("data")) \
            .select("data.*")\
            .writeStream \
            .foreachBatch(foreach_batch_function) \
            .trigger(processingTime='30 seconds') \
            .start()

        query.awaitTermination()

    def insertIntoHiveTable(self, df, table_name):
        pass

    def createStructure(self, spark):
        super().createStructure(spark)