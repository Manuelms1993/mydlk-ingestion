from abc import abstractmethod

from db.DB import rawdb
from db.tables import create_raw_imdb_schema, create_raw_films_schema


class Source:

    @abstractmethod
    def ingestion(self, spark):
        pass

    def insertIntoHiveTable(self, df, table_name):
        df.write.insertInto(table_name, overwrite = True)

    def createStructure(self, spark):
        spark.sql(rawdb)
        spark.sql("DROP TABLE IF EXISTS raw.films")
        spark.sql("DROP TABLE IF EXISTS raw.imdb")
        spark.sql(create_raw_imdb_schema)
        spark.sql(create_raw_films_schema)