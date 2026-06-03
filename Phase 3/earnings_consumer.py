from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'ride.completed',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id='earnings-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

driver_stats = {}

print("Calculating driver earnings...\n")

for message in consumer:
    ride = message.value

    driver_id = ride['driver_id']

    if driver_id not in driver_stats:
        driver_stats[driver_id] = {
            "rides": 0,
            "earnings": 0
        }

    driver_stats[driver_id]["rides"] += 1
    driver_stats[driver_id]["earnings"] += 5

    print(
        f"Driver: {driver_id} | "
        f"Completed Rides: {driver_stats[driver_id]['rides']} | "
        f"Earnings: ${driver_stats[driver_id]['earnings']}"
    )