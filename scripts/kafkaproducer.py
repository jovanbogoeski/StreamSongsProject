from kafka import KafkaConsumer
import json

# Configuration for Kafka
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'events_topic'

def main():
    # Set up the Kafka consumer
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    print("Starting to consume events...")

    try:
        for message in consumer:
            event = message.value  # Extract the event data
            print(f"Received event structure: {event}")  # Print the entire event structure

    except KeyboardInterrupt:
        print("Consumer interrupted. Exiting...")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
