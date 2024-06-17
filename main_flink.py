








#self.env = StreamExecutionEnvironment.get_execution_environment()
#self.env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
#self.env.set_parallelism(1)
#self.env.add_jars("file:///Users/manuelmontero/MM_DLK/mydlk-ingestion/resources/flink-sql-connector-kafka-1.16.0.jar")

#deserialization_schema = SimpleStringSchema()

#kafkaSource = FlinkKafkaConsumer(
#    topics='actors_topic',
#    deserialization_schema=deserialization_schema,
#    properties={'bootstrap.servers': '127.0.0.1:9092', 'group.id': 'test'}
#)

#ds = self.env.add_source(kafkaSource).print()
#ds : DataStream = self.env.add_source(kafkaSource)
#ds.sink_to()
#self.env.execute('kafkaread')