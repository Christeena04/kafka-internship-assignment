from kafka import KafkaProducer
import csv
import json

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

with open('rides.csv', 'r') as file:
    reader = csv.DictReader(file)

    for row in reader:
        producer.send(
            'ride.events',
            key=row['ride_id'].encode('utf-8'),
            value=row
        )

        print("Sent:", row)

producer.flush()
print("All ride events sent successfully!")