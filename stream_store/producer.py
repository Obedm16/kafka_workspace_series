import json
import uuid
from confluent_kafka import Producer




producer_config = {
  'bootstrap.servers': 'localhost:9092'
}

producer = Producer(producer_config)

def delivery_report(err, msg):
    if err:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} {msg.value().decode('utf-8')} [{msg.partition()}]: at offset {msg.offset()}")
        # print(dir(msg))


order = {
  "order_id": str(uuid.uuid4()),
  "user": "john_doe",
  "product": "laptop",
  "quantity": 1
}

value = json.dumps(order).encode('utf-8')
producer.produce(topic='orders', value=value)

producer.produce(
  topic='orders',
  value=value,
  callback=delivery_report
)
# print(f"Produced order: {order}")
producer.flush()
