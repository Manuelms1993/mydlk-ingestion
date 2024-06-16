from pyspark.sql import DataFrame

from batch.sources.source import Source


class FileSource(Source):

    files = {
        'raw.films': 'file:/Users/manuelmontero/MM_DLK/MyDatalake/datasets/films.csv',
        'raw.imdb': "file:/Users/manuelmontero/MM_DLK/MyDatalake/datasets/imdb.csv"
    }

    def ingestion(self, spark):

        for key, value in self.files.items():
            df : DataFrame = spark.read.csv(value, header=True, inferSchema=True)
            #df.limit(5).show()
            #df.printSchema()
            self.insertIntoHiveTable(df, key)