from pyspark.sql import SparkSession, DataFrame

from db.DB import rawdb
from db.tables import create_raw_imdb_schema, create_raw_films_schema

if __name__ == "__main__":

    files = {
        'raw.films': 'file:/Users/manuelmontero/MM_DLK/MyDatalake/datasets/films.csv',
        'raw.imdb': "file:/Users/manuelmontero/MM_DLK/MyDatalake/datasets/imdb.csv"
    }

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

    # createdb
    spark.sql(rawdb)
    spark.sql("DROP TABLE IF EXISTS raw.films")
    spark.sql("DROP TABLE IF EXISTS raw.imdb")
    spark.sql(create_raw_imdb_schema)
    spark.sql(create_raw_films_schema)

    # insert into hive
    for key, value in files.items():
        df: DataFrame = spark.read.csv(value, header=True, inferSchema=True)
        # df.limit(5).show()
        # df.printSchema()
        df.write.insertInto(key, overwrite = True)

    # test
    spark.table("raw.films").limit(5).show()
    spark.table("raw.imdb").limit(5).show()

    # end
    spark.stop()