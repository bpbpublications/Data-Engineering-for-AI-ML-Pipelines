
import json
import snowflake.connector
from kafka import KafkaConsumer

# Step 1: Connect to Kafka
consumer = KafkaConsumer(
    'backend-logs',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True
)

print("Connected to Kafka!")

# Step 2: Pre-process the Data
logs = []
for message in consumer:
    log = json.loads(message.value.decode('utf-8'))  # Decode the JSON data from Kafka
    # Assume log entries include user_id, activity_type, video_id, session_id
    processed_log = {
        "timestamp": log["timestamp"],
        "user_id": log["user_id"],
        "activity_type": log["activity_type"],
        "video_id": log.get("video_id", None),  # video_id might not be applicable for all activities like Login
        "session_id": log["session_id"]
    }
    logs.append(processed_log)
    
    if len(logs) >= 1000:  # Process in batches of 1000 for example
        break

print("Logs collected and pre-processed!")



# Step 3: Write to Snowflake
conn = snowflake.connector.connect(
    user='YOUR_USERNAME',
    password='YOUR_PASSWORD',
    account='YOUR_ACCOUNT.snowflakecomputing.com',
    warehouse='YOUR_WAREHOUSE',
    database='YOUR_DATABASE',
    schema='YOUR_SCHEMA'
)

cursor = conn.cursor()



# Writing data to Snowflake
try:
    for log in logs:
        cursor.execute("""
            INSERT INTO user_activity_logs (timestamp, user_id, activity_type, video_id, session_id)
            VALUES (%s, %s, %s, %s, %s);
        """, (log["timestamp"], log["user_id"], log["activity_type"], log["video_id"], log["session_id"]))
    print("Data written to Snowflake successfully!")
finally:
    cursor.close()
    conn.close()

# Close Kafka consumer
consumer.close()