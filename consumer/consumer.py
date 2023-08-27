from confluent_kafka import Consumer, KafkaError

# Kafka consumer configuration
consumer_config = {
    'bootstrap.servers': 'localhost:9091',  # Replace with your Kafka broker address
    'group.id': 'test-group',
    'auto.offset.reset': 'earliest'  # Start consuming from the beginning of the topic
}

# Kafka topic to consume from
topic = 'traffic_data'  # Use the same topic name as in producer.py

# Create a Kafka consumer instance
consumer = Consumer(consumer_config)

# Subscribe to the topic
consumer.subscribe([topic])

try:
    while True:
        msg = consumer.poll(1.0)  # Poll for messages with a timeout of 1 second
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print('Reached end of topic partition')
            else:
                print('Error:', msg.error())
        else:
            # Print the received message key and value
            print(f'Received message: key={msg.key()}, value={msg.value()}')

except KeyboardInterrupt:
    pass
finally:
    consumer.close()