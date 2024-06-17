from abc import abstractmethod

from db.DB import streamdb
from db.tables import create_stream_actors


class Source:

    @abstractmethod
    def ingestion(self, spark):
        pass

    def insertIntoHiveTable(self, df, table_name):
        df.write.insertInto(table_name, overwrite=True)

    def createStructure(self, spark):
        spark.sql(streamdb)
        spark.sql("DROP TABLE IF EXISTS streaming.actors")
        spark.sql(create_stream_actors)