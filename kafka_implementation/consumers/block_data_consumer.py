import os
import sys
from confluent_kafka import Consumer
from datetime import datetime
import json
import pandas as pd
import sqlalchemy as sa
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bitcoin_etl_helpers import *
from bitcoin_etl_main import transform_n_load_block_data



# Kafka config
BLOCK_TOPIC = "bitcoin_block_data"
BOOTSTRAP_SERVERS = 'localhost:9092'

conf = {
    'bootstrap.servers': BOOTSTRAP_SERVERS,
    'group.id': 'bitcoin_block_consumer_group',
    'group.protocol':'classic',
    'enable.auto.commit': 'false',
    'auto.offset.reset': 'earliest',
    'fetch.min.bytes': 8192, # wait until at least 8kilobytes of data are available across all partitions assigned to the consumer before sending a response
    'fetch.max.bytes': 1048576, # maximum bytes to fetch in one request, 1MB
    #'fetch.max.wait.ms': 500,
    'heartbeat.interval.ms': 3000,
    'session.timeout.ms': 45000,
    'max.poll.interval.ms': 300000,
    #'max.poll.records': 500,
    'request.timeout.ms': 30000,
    'allow.auto.create.topics': 'false'
}

# Create Kafka consumer
def consume_block_data():
    consumer = Consumer(conf)
    consumer.subscribe([BLOCK_TOPIC])
    logger.info(f"Subscribed to Kafka topic: {BLOCK_TOPIC}")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                logger.info("Waiting for new block data...")
                time.sleep(1)
                continue
            if msg.error():
                logger.error(f"Kafka error: {msg.error()}")
                continue

            try:
                timestamp_type, timestamp_value = msg.timestamp()
                timestamp_str = (
                    datetime.fromtimestamp(timestamp_value / 1000.0).strftime('%Y-%m-%d %H:%M:%S')
                    if timestamp_value else "N/A"
                )

                value = msg.value().decode('utf-8')
                logger.info(f"Consumed block from topic: {msg.topic()} | timestamp: {timestamp_str}")

                data = json.loads(value)
                transform_n_load_block_data(block_data=data)

                # Commit offset only after successful processing
                consumer.commit(asynchronous=False)
                logger.info(f"Offset committed for partition {msg.partition()} at offset {msg.offset()}")
                
            except Exception:
                logger.error("Error while processing block message")
                logger.error(traceback.format_exc())

    except KeyboardInterrupt:
        logger.info("Consumer interrupted by user.")
    finally:
        consumer.close()
        logger.info("Kafka consumer closed.")

if __name__ == "__main__":
    consume_block_data()