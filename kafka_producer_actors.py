from datetime import datetime

from kafka import KafkaProducer
import json
import pandas as pd
import time
from db.topics import actors_topic
import logging
logging.basicConfig(level=logging.INFO)

producer = KafkaProducer(
    bootstrap_servers='127.0.0.1:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Function to read CSV and send records to Kafka
def send_to_kafka():
    # Read CSV file into Pandas DataFrame
    df = pd.read_csv("file:/Users/manuelmontero/MM_DLK/MyDatalake/datasets/actors.csv")

    # Iterate over each row and send to Kafka
    for index, row in df.iterrows():
        record = {
            'person_id': row['person_id'],
            'name': row['name'],
            'characterType': row['character'],
            'role': row['role'],
            'timeMark': datetime.now().strftime("%Y%m%d %H%M%S")
        }
        producer.send(actors_topic, value=record)
        print(f"Sent message: {record}")
        producer.flush()
        time.sleep(1)

send_to_kafka()
producer.close()