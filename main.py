# Import necessary modules
from download_kaggle import download_kaggle_dataset
from kafka_producer import produce_to_kafka
from loguru import logger

# Define dataset details
DATASET_OWNER_SLUG = "carrie1"
DATASET_NAME = "ecommerce-data"
DOWNLOAD_PATH = "./data"
CSV_FILE_PATH = "./data/data.csv"

def main():
    """Main function to orchestrate the data pipeline."""
    logger.info("Starting the data pipeline: Kaggle → Kafka")
    download_kaggle_dataset(DATASET_OWNER_SLUG, DATASET_NAME, DOWNLOAD_PATH)

    logger.info("Kaggle dataset downloaded successfully. Sending data to Kafka.")
    produce_to_kafka(CSV_FILE_PATH)

    logger.success("Pipeline execution completed successfully.")

if __name__ == "__main__":
    main()