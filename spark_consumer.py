import os
os.environ["HADOOP_HOME"] = "C:\\hadoop"

import logging
import config
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define schema for incoming data
schema = StructType([
    StructField("InvoiceNo", StringType(), True),
    StructField("StockCode", StringType(), True),
    StructField("Description", StringType(), True),
    StructField("Quantity", IntegerType(), True),
    StructField("InvoiceDate", StringType(), True),
    StructField("UnitPrice", DoubleType(), True),
    StructField("CustomerID", StringType(), True),
    StructField("Country", StringType(), True)
])

def initialize_snowflake_table(spark):
    """Creates or replaces the Snowflake table before inserting new data."""
    query = """
        CREATE OR REPLACE TABLE ALL_ECOMMERCE_DATA (
            InvoiceNo STRING,
            StockCode STRING,
            Description STRING,
            Quantity INT,
            InvoiceDate STRING,
            UnitPrice DOUBLE,
            CustomerID STRING,
            Country STRING
        )
    """
    try:
        spark.read.format("snowflake").options(**config.SNOWFLAKE_OPTIONS).option("query", query).load()
        logger.info("Snowflake table ALL_ECOMMERCE_DATA created/replaced successfully.")
    except Exception as exc:
        logger.error(f"Error initializing Snowflake table: {exc}")

def write_batch_to_snowflake(df, epoch_id):
    """Writes a batch of data to Snowflake."""
    try:
        df.write \
          .format("snowflake") \
          .options(**config.SNOWFLAKE_OPTIONS) \
          .option("dbtable", "ALL_ECOMMERCE_DATA") \
          .mode("append") \
          .save()
        logger.info(f"Batch {epoch_id} successfully written to Snowflake.")
    except Exception as exc:
        logger.error(f"Error writing batch {epoch_id} to Snowflake: {exc}")

def main():
    """Sets up Spark Streaming to process Kafka data and write to Snowflake."""
    spark = SparkSession.builder.appName("KafkaToSnowflake") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.executor.memory", "4g") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0," \
                "net.snowflake:spark-snowflake_2.12:3.0.0") \
        .getOrCreate()

    # Initialize or replace the Snowflake table before inserting new data
    initialize_snowflake_table(spark)

    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", config.KAFKA_SERVER) \
        .option("subscribe", config.KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()

    ecommerce_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json(col("json_str"), schema).alias("data")) \
        .select("data.*")

    query = ecommerce_df.writeStream \
        .foreachBatch(write_batch_to_snowflake) \
        .outputMode("append") \
        .option("checkpointLocation", "./checkpoint_all_data") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()
