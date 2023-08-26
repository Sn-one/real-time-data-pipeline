from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SparkSession
import json

# Kafka configuration
kafka_server = 'kafka-server:9092'  # Update with your Kafka server's address
kafka_topic = 'real-time-data'  # Update with the Kafka topic name

# PostgreSQL and TimescaleDB configuration
pg_url = 'jdbc:postgresql://postgres-server:5432/your_database_name'  # Update with your PostgreSQL server and database
pg_properties = {
    'user': 'your_username',  # Update with your PostgreSQL username
    'password': 'your_password',  # Update with your PostgreSQL password
    'driver': 'org.postgresql.Driver'
}

def process_stream(rdd):
    if not rdd.isEmpty():
        spark = SparkSession.builder.appName("RealTimeDataProcessing").getOrCreate()
        
        # Convert RDD to DataFrame
        df = spark.read.json(rdd)
        
        # Transform and process the data (you can define your processing logic here)
        processed_df = df.select(
            "highway",
            "type",
            "color",
            "direction",
            "speed",
            "timestamp"
        )
        
        # Write processed data to PostgreSQL and TimescaleDB
        processed_df.write.jdbc(url=pg_url, table="processed_data", mode="append", properties=pg_properties)

if __name__ == "__main__":
    sc = SparkContext(appName="RealTimeDataProcessing")
    ssc = StreamingContext(sc, 1)  # Batch interval of 1 second
    
    # Create a Kafka stream
    kafka_stream = KafkaUtils.createStream(
        ssc, kafka_server, "real-time-data-processing-group", {kafka_topic: 1})
    
    # Extract the value field (assuming the Kafka producer sends JSON data)
    json_stream = kafka_stream.map(lambda x: json.loads(x[1]))
    
    # Process and transform the data
    json_stream.foreachRDD(process_stream)
    
    # Start the Spark Streaming context
    ssc.start()
    ssc.awaitTermination()
