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
    
    def send_msg(self, msg):
        try:
            future = self.producer.send(self.topic, msg)
            self.producer.flush()
            future.get(timeout=60)
            print("message sent successfully...")
            return {'status_code': 200, 'error': None }
        except Exception as e:
            return e
        

broker = 'localhost:9092'
topic = 'test-topic'
message_producer = MessageProducer(broker, topic)

data = {'name':'abc', 'email':'abc@abc.com'}
resp = message_producer.send_msg(data)
print(resp)