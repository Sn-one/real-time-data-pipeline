import json
import random
import time
from confluent_kafka import Producer

# List of vehicle types and colors
vehicle_types = ["motorcycle", "bus", "car", "bicycle", "truck"]
colors = ["red", "blue", "green", "yellow", "white"]
speed_ranges = {
    "motorcycle": (60, 150),
    "bus": (40, 100),
    "car": (80, 150),
    "bicycle": (10, 30),  # Fixed the typo in "cycle" to "bicycle"
    "truck": (60, 120)
}
highways = ["N1", "N2", "N3", "N4", "N5"]
directions = ["northbound", "southbound", "eastbound", "westbound"]


# Kafka producer configuration
producer_config = {
    'bootstrap.servers': 'localhost:9091',  # Replace with your Kafka broker address
    'client.id': 'traffic-producer'
}

# Kafka topic to send traffic data to
topic = 'traffic_data'

# Create a Kafka producer instance
producer = Producer(producer_config)

# Function to generate random traffic data
def generate_traffic_data():
    while True:
        vehicle = {
            "highway": random.choice(highways),
            "type": random.choice(vehicle_types),
            "color": random.choice(colors),
            "direction": random.choice(directions),
            "speed": random.randint(*speed_ranges.get("car")),  # Adjust based on the chosen vehicle type
            "timestamp": int(time.time())
        }
        yield vehicle
        time.sleep(1)  # Generate data every second

if __name__ == "__main__":
    data_generator = generate_traffic_data()

    try:
        for _ in range(30):  # Generate data for 5 minutes (300 seconds)
            vehicle_data = next(data_generator)
            key = "vehicle_" + str(time.time())  # Use a unique key for each message

            # Send the data to the Kafka topic
            producer.produce(topic, key=key, value=json.dumps(vehicle_data))
            print("Data sent successfully")

            time.sleep(1)  # Generate data every second
    except Exception as e:
        print("Error sending data:", str(e))
    finally:
        producer.flush()
