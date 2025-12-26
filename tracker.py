from confluent_kafka import Consumer
import json

consumer_config = {
  'bootstrap.servers': 'localhost:9092',
  'group.id': 'order-tracker',
  'auto.offset.reset': 'earliest',
  'enable.auto.commit': False
}

consumer = Consumer(consumer_config)

consumer.subscribe(['orders']) #one consumer can subscribe to multiple topics

print("🟢 Consumer is running and listening to orders topic")
try:
    while True:
        msg = consumer.poll(1.0)  #timeout of 1 second
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        value = msg.value().decode('utf-8')
        order = json.loads(value) #deserialize JSON string to Python dict
        print(f"📦Received order: {value} from topic: {msg.topic()} partition: {msg.partition()} at offset: {msg.offset()}")
        consumer.commit(msg)  #manually commit the offset after processing

except KeyboardInterrupt:
    print("🛑 Consumer is shutting down")