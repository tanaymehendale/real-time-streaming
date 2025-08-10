from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',  # This only works if 9092 is exposed to host
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

data = {"test": "message"}
producer.send("users_created", value=data)
producer.flush()
print("Message sent!")
