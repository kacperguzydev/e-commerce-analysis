import pandas as pd
import json
from confluent_kafka import Producer
from cerberus import Validator
from loguru import logger
from config import KAFKA_TOPIC, KAFKA_SERVER

# Schema definition for data validation
schema = {
    "InvoiceNo": {'type': 'string', 'nullable': True, 'empty': True},
    "StockCode": {'type': 'string', 'nullable': True, 'empty': True},
    "Description": {'type': 'string', 'nullable': True, 'empty': True},
    "Quantity": {'type': 'integer', 'nullable': True},
    "InvoiceDate": {'type': 'string', 'nullable': True, 'empty': True},
    "UnitPrice": {'type': 'float', 'nullable': True},
    "CustomerID": {'type': 'string', 'nullable': True, 'empty': True},
    "Country": {'type': 'string', 'nullable': True, 'empty': True}
}

validator = Validator(schema)

def delivery_report(err, msg):
    """Handles Kafka message delivery reports."""
    if err:
        logger.error(f"Message delivery failed: {err}")

def produce_to_kafka(csv_path: str):
    """Reads data from CSV, validates, and sends it to Kafka."""
    producer_conf = {
        'bootstrap.servers': KAFKA_SERVER,
        'queue.buffering.max.messages': 1000000,
        'batch.size': 16384,
        'linger.ms': 500,
        'compression.type': 'gzip'
    }

    producer = Producer(producer_conf)

    df = pd.read_csv(csv_path, encoding='ISO-8859-1', dtype={'CustomerID': str}).fillna('')
    df['CustomerID'] = df['CustomerID'].str.replace('.0', '', regex=False)
    df.drop_duplicates(inplace=True)
    df = df[(df['Quantity'] > 0) & (df['UnitPrice'] > 0)]

    valid_records = df[df.apply(lambda x: validator.validate(x.to_dict()), axis=1)]
    invalid_count = len(df) - len(valid_records)
    logger.warning(f"Filtered out {invalid_count} invalid records.")

    produced_count = 0
    for record in valid_records.to_dict(orient='records'):
        producer.produce(
            KAFKA_TOPIC,
            json.dumps(record).encode('utf-8'),
            callback=delivery_report
        )
        produced_count += 1
        if produced_count % 10000 == 0:
            producer.poll(1)

    producer.flush()
    logger.info(f"Successfully sent {produced_count} records to Kafka.")