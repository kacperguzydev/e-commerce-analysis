import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Snowflake connection options
SNOWFLAKE_OPTIONS = {
    "sfURL": os.getenv("SNOWFLAKE_URL"),
    "sfDatabase": os.getenv("SNOWFLAKE_DB"),
    "sfSchema": os.getenv("SNOWFLAKE_SCHEMA"),
    "sfWarehouse": os.getenv("SNOWFLAKE_WH"),
    "sfRole": os.getenv("SNOWFLAKE_ROLE"),
    "sfUser": os.getenv("SNOWFLAKE_USER"),
    "sfPassword": os.getenv("SNOWFLAKE_PASSWORD"),
}

# Kafka connection settings
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
KAFKA_SERVER = os.getenv("KAFKA_SERVER")