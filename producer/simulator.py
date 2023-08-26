import json
import random
import time
from kafka import KafkaProducer

# List of vehicle types and colors
vehicle_types = ["motorcycle", "bus", "car", "bicycle", "truck"]
colors = ["red", "blue", "green", "yellow", "white"]
speed_ranges = {
    "motorcycle": (60, 150),
    "bus": (40, 100),
    "car": (80, 150),
    "cycle": (10, 30),
    "truck": (60, 120)
}
highways = ["N1", "N2", "N3", "N4", "N5"]
directions = ["northbound", "southbound", "eastbound", "westbound"]

# Kafka producer configuration
kafka_server = 'kafka-server:9092'  # Update with your Kafka server's address
kafka_topic = 'real-time-data'  # Update with the Kafka topic name

# Create Kafka producer
producer = KafkaProducer(bootstrap_servers=kafka_server,
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Function to generate random traffic data
def generate_traffic_data():
    while True:
        vehicle = {
            "highway": random.choice(highways),
            "type": random.choice(vehicle_types),
            "color": random.choice(colors),
            "direction": random.choice(directions),
            "speed": random.randint(40, 120),  # Speed in km/h
            "timestamp": int(time.time())  # Use Unix timestamp for time
        }
        
        # Send data to Kafka topic
        producer.send(kafka_topic, value=vehicle)
        print("Sent data to Kafka:", vehicle)
        
        time.sleep(1)  # Generate data every second

if __name__ == "__main__":
    data_generator = generate_traffic_data()
    try:
        for _ in range(300):  # Generate data for 5 minutes (300 seconds)
            next(data_generator)
    except KeyboardInterrupt:
        print("Data generation stopped.")
    finally:
        producer.close()
