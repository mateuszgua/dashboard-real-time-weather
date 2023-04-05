import json
from kafka import KafkaProducer


class MessageProducer:
    broker = ""
    topic = ""
    producer = None

    def __init__(self, broker, topic) -> None:
        self.broker = broker
        self.topic = topic
        self.producer = KafkaProducer(bootstrap_servers=self.broker,
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                         acks='all',
                         retries=3)