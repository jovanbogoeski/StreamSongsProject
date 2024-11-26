import os
from utils import initialize_spark, read_from_kafka, parse_topic, write_stream
from schemas import schema_mapping
import logging

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPICS = os.getenv("KAFKA_TOPICS", "auth_events,listen_events,page_view_events,status_change_events").split(",")
OUTPUT_BASE_PATH = os.getenv("OUTPUT_PATH", "/mnt/c/Users/Jovan Bogoevski/StreamsSongs/processed_output")
CHECKPOINT_BASE_PATH = os.getenv("CHECKPOINT_PATH", "/mnt/c/Users/Jovan Bogoevski/StreamsSongs/processed_output/checkpoints")
PROCESSING_INTERVAL = os.getenv("PROCESSING_INTERVAL", "1 minute")

def main():
    # Initialize Spark session
    spark = initialize_spark("KafkaSparkStreaming")

    # Read raw Kafka stream
    raw_stream = read_from_kafka(spark, KAFKA_BROKER, KAFKA_TOPICS)

    # Process and write streams
    queries = []
    for topic in KAFKA_TOPICS:
        schema = schema_mapping.get(topic)
        if not schema:
            logger.warning(f"Schema for topic '{topic}' not found. Skipping.")
            continue

        parsed_df = parse_topic(raw_stream, topic, schema)
        if parsed_df is None:
            logger.error(f"Failed to parse topic '{topic}'. Skipping.")
            continue

        output_path = os.path.join(OUTPUT_BASE_PATH, topic)
        checkpoint_path = os.path.join(CHECKPOINT_BASE_PATH, topic)

        query = write_stream(parsed_df, output_path, checkpoint_path, PROCESSING_INTERVAL)
        if query:
            queries.append(query)

    # Await termination
    for query in queries:
        query.awaitTermination()

if __name__ == "__main__":
    main()
