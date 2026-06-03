from kafka import KafkaConsumer, KafkaProducer
import json

consumer = KafkaConsumer(
    'ride.completed',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id='earnings-group new',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
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

    earnings_record = {
        "driver_id": driver_id,
        "completed_rides": driver_stats[driver_id]["rides"],
        "earnings": driver_stats[driver_id]["earnings"]
    }

    producer.send(
        "driver.earnings",
        value=earnings_record
    )

    print("Published:", earnings_record)

producer.flush()