import os
from utils import initialize_spark, read_from_kafka, process_topic
from schemas import schema_mapping
import logging

# Configure logging
def setup_logging():
    """Sets up the logging configuration."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.StreamHandler()  # Logs to the console
        ],
    )
    logger = logging.getLogger(__name__)
    logger.info("Logging has been configured.")
    return logger

# Initialize logging
logger = setup_logging()

# Configuration
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPICS = os.getenv(
    "KAFKA_TOPICS", "auth_events,listen_events,page_view_events,status_change_events"
).split(",")
OUTPUT_BASE_PATH = os.getenv("OUTPUT_PATH", "/home/jovanbogoeski/out")
CHECKPOINT_BASE_PATH = os.getenv("CHECKPOINT_PATH", "/home/jovanbogoeski/out/checkpoints")
PROCESSING_INTERVAL = os.getenv("PROCESSING_INTERVAL", "1 minute")

def ensure_directory_exists(path):
    """Ensure the specified directory exists."""
    try:
        if not os.path.exists(path):
            os.makedirs(path)
            logger.info(f"Created directory: {path}")
        else:
            logger.info(f"Directory already exists: {path}")
    except Exception as e:
        logger.error(f"Failed to create directory '{path}': {e}")
        raise

def main():
    """Main entry point of the script."""
    try:
        # Initialize Spark session
        logger.info("Initializing Spark session...")
        spark = initialize_spark("KafkaSparkStreaming")

        # Validate and create base output and checkpoint directories
        logger.info("Validating output and checkpoint directories...")
        ensure_directory_exists(OUTPUT_BASE_PATH)
        ensure_directory_exists(CHECKPOINT_BASE_PATH)

        # Read raw Kafka stream
        logger.info(f"Reading data from Kafka topics: {KAFKA_TOPICS}...")
        raw_stream = read_from_kafka(spark, KAFKA_BROKER, KAFKA_TOPICS)

        # Process each topic
        queries = []
        for topic in KAFKA_TOPICS:
            logger.info(f"Processing topic: {topic}")
            
            # Get the schema for the current topic
            schema = schema_mapping.get(topic)
            if not schema:
                logger.warning(f"Schema for topic '{topic}' not found. Skipping.")
                continue

            # Define output and checkpoint paths
            output_path = os.path.join(OUTPUT_BASE_PATH, topic)
            checkpoint_path = os.path.join(CHECKPOINT_BASE_PATH, topic)

            # Ensure directories exist
            ensure_directory_exists(output_path)
            ensure_directory_exists(checkpoint_path)

            # Process the topic (parse, transform, and write)
            query = process_topic(raw_stream, topic, schema, output_path, checkpoint_path)
            if query:
                queries.append(query)

        # Await termination for all streaming queries
        for query in queries:
            query.awaitTermination()

    except Exception as e:
        logger.error(f"An error occurred in the main function: {e}")
        raise

if __name__ == "__main__":
    logger.info("Starting the Kafka streaming application...")
    main()
