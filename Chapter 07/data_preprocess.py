
import json
logs = []
for message in consumer:
    log = json.loads(message.value.decode('utf-8'))  # Decode the JSON data from Kafka
    # Example preprocessing: convert to a suitable JSON format if needed
    processed_log = {
        "timestamp": log["timestamp"],
        "level": log["level"],
        "message": log["message"]
    }
    logs.append(processed_log)
    
    if len(logs) >= 1000:  # Process in batches of 1000
        break
print("Logs collected and pre-processed!")

