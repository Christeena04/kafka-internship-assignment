from kafka import KafkaConsumer
import json

order_count = {}

consumer = KafkaConsumer(
    'ecommerce.orders',
    bootstrap_servers='localhost:9092',
    group_id='order-analytics',
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Consumer started...\n")

for message in consumer:
    order = message.value

    user_id = order['user_id']

    order_count[user_id] = order_count.get(user_id, 0) + 1

    print(f"Received Order: {order['order_id']}")
    print(f"User {user_id} -> Total Orders: {order_count[user_id]}")
    print("-" * 40)