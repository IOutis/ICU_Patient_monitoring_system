from kafka import KafkaProducer
import json

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer=lambda m: json.dumps(m).encode('ascii'))
def send_message(message:dict):
    producer.send('my_topic',message)
    producer.flush()