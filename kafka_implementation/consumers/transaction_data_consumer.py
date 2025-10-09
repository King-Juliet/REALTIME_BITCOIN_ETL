import os
import sys
from confluent_kafka import Consumer
from datetime import datetime
import json
import pandas as pd
import sqlalchemy as sa
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bitcoin_etl_helpers import *
from bitcoin_etl_main import transform_n_load_transaction_data


# Kafka topics
BLOCK_TOPIC = "bitcoin_block_data"
TRANSACTION_TOPIC = "bitcoin_transaction_data"
BOOTSTRAP_SERVERS = 'localhost:9092'

conf = {
    'bootstrap.servers': BOOTSTRAP_SERVERS,
    'group.id': 'bitcoin_consumer_group',
    'group.protocol':'classic',
    'enable.auto.commit': 'false',
    'auto.offset.reset': 'earliest',
    'fetch.min.bytes': 8192, # wait until at least 8kilobytes of data are available across all partitions assigned to the consumer before sending a response
    'fetch.max.bytes': 1048576, # maximum bytes to fetch in one request, 1MB
    'heartbeat.interval.ms': 3000,
    'session.timeout.ms': 45000,
    'max.poll.interval.ms': 300000,
    'request.timeout.ms': 30000,
    'allow.auto.create.topics': 'false'
}


def consume_transaction_data():
    consumer = Consumer(conf)
    consumer.subscribe([BLOCK_TOPIC, TRANSACTION_TOPIC])
    logger.info(f"Subscribed to topics: {BLOCK_TOPIC}, {TRANSACTION_TOPIC}")

    # Store temporary data in memory until both block and txs for same height arrive
    blocks_buffer = {}       # {height: block_data}
    transactions_buffer = {} # {height: detailed_txs}

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                logger.info("Waiting for new messages...")
                time.sleep(1)
                continue
            if msg.error():
                logger.error(f"Kafka error: {msg.error()}")
                continue

            topic = msg.topic()
            timestamp_type, timestamp_value = msg.timestamp()
            timestamp_str = (
                datetime.fromtimestamp(timestamp_value / 1000.0).strftime('%Y-%m-%d %H:%M:%S')
                if timestamp_value else "N/A"
            )

            try:
                value = msg.value().decode('utf-8')
                data = json.loads(value)

                if topic == BLOCK_TOPIC:
                    block_height = data.get("height")
                    if not block_height:
                        logger.warning("Received block without height — skipping")
                        continue

                    blocks_buffer[block_height] = data
                    logger.info(f"Block {block_height} received at {timestamp_str}")

                    # Check if we already have transactions for this block
                    if block_height in transactions_buffer:
                        txs = transactions_buffer.pop(block_height)
                        logger.info(f"Pairing and loading block {block_height} with {len(txs)} txs")
                        transform_n_load_transaction_data(block_data=data, detailed_txs=txs)
                        blocks_buffer.pop(block_height, None)

                elif topic == TRANSACTION_TOPIC:
                    # detailed_txs is expected to be a list of txs from producer
                    if not isinstance(data, list) or len(data) == 0:
                        logger.warning("Received empty transaction batch — skipping")
                        continue

                    # extract block height safely
                    sample_tx = data[0]
                    block_height = sample_tx.get("status", {}).get("block_height") or sample_tx.get("block_height")
                    if not block_height:
                        logger.warning("Received transactions without block height — skipping")
                        continue

                    transactions_buffer[block_height] = data
                    logger.info(f"Transactions for block {block_height} received at {timestamp_str}")

                    # Check if corresponding block already arrived
                    if block_height in blocks_buffer:
                        block_data = blocks_buffer.pop(block_height)
                        txs = transactions_buffer.pop(block_height)
                        logger.info(f"Pairing and loading block {block_height} with {len(txs)} txs")
                        transform_n_load_transaction_data(block_data=block_data, detailed_txs=txs)
                        
                        # Commit offset only after successful processing
                        consumer.commit(asynchronous=False)
                        logger.info(f"Offset committed for partition {msg.partition()} at offset {msg.offset()}")
            except Exception:
                logger.error("Error processing message")
                logger.error(traceback.format_exc())

    except KeyboardInterrupt:
        logger.info("Consumer interrupted by user.")
    finally:
        consumer.close()
        logger.info("Kafka consumer closed.")


if __name__ == "__main__":
    consume_transaction_data()
