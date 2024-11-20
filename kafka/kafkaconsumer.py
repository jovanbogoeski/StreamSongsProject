from kafka import KafkaConsumer
import json
import pandas as pd
import os

# Configuration
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPICS = ['auth_events', 'listen_events', 'page_view_events', 'status_change_events']
OUTPUT_DIRECTORY = '/mnt/c/Users/Jovan Bogoevski/StreamsSongs/test_kafka/'
os.makedirs(OUTPUT_DIRECTORY, exist_ok=True)  # Ensure output directory exists

# Set up the Kafka consumer
consumer = KafkaConsumer(
    *KAFKA_TOPICS,
    bootstrap_servers=[KAFKA_BROKER],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='csv_output_consumer_group'
)

# Prepare data storage for each topic
dataframes = {topic: [] for topic in KAFKA_TOPICS}

print("Starting message consumption and saving to CSV...")

try:
    for message in consumer:
        topic = message.topic
        print(f"Received message from {topic}")

        # Decode and parse JSON message
        try:
            message_json = json.loads(message.value.decode('utf-8'))
            dataframes[topic].append(message_json)

            # Save to CSV after 100 messages per topic
            if len(dataframes[topic]) >= 100:
                df = pd.DataFrame(dataframes[topic])
                csv_filename = os.path.join(OUTPUT_DIRECTORY, f"{topic}.csv")
                
                # Append to CSV if it exists, otherwise create a new one
                if os.path.exists(csv_filename):
                    df.to_csv(csv_filename, mode='a', header=False, index=False)
                else:
                    df.to_csv(csv_filename, index=False)
                
                print(f"Saved {len(dataframes[topic])} messages to {csv_filename}")
                dataframes[topic] = []  # Clear the stored messages for the topic

        except json.JSONDecodeError:
            print(f"Failed to decode message from {topic} as JSON.")

except Exception as e:
    print("Error during message consumption:", e)
finally:
    consumer.close()
    print("Consumer closed.")
