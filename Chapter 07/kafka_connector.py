
from kafka import KafkaConsumer
# Create a Kafka consumer to connect to the Kafka cluster
consumer = KafkaConsumer(
    'backend-logs',                   # Kafka topic
    bootstrap_servers=['localhost:9092'],   # Kafka server
    auto_offset_reset='earliest',     # Start reading at the earliest message
    enable_auto_commit=True
)
print("Connected to Kafka!")

