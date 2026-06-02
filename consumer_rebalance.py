from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'ecommerce.orders',
    bootstrap_servers='localhost:9092',
    group_id='order-analytics',
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Consumer started...")

for message in consumer:
    print(
        f"Partition: {message.partition}, "
        f"Offset: {message.offset}, "
        f"Order: {message.value['order_id']}"
    )