import json
import time
import logging
import csv
from confluent_kafka import Producer
import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import your blockchain ETL functions
from bitcoin_etl_helpers import *
from bitcoin_etl_main import extract_block


# Kafka Configuration
BOOTSTRAP_SERVERS = 'localhost:9092'
BLOCK_TOPIC = "bitcoin_block_data"
TRANSACTION_TOPIC = "bitcoin_transaction_data"

PRODUCER_CONFIG = {
                    'bootstrap.servers': BOOTSTRAP_SERVERS, 
                     'client.id': 'bitcoin_producer',
                     'acks': 'all',
                     'enable.idempotence': True,
                     'retries': 2,
                     'max.in.flight.requests.per.connection': 1,
                     'delivery.timeout.ms': 120000,
                     'batch.size': 16384, # produce request can carry up to 16 KB per partition
                     'linger.ms': 5,
                     'request.timeout.ms': 30000,
                     'retry.backoff.max.ms': 1 # wait 1s before retrying a failed produce request
                     }


# Kafka Producer setup
producer = Producer(PRODUCER_CONFIG)

# CSV file path for storing metrics
METRICS_FILE = "kafka_producer_metrics.csv"
# # log producer metrics every X seconds
# def log_producer_metrics(producer):
#     """Periodically log Kafka producer metrics for tuning."""
#     metrics = producer.metrics()
#     for node, data in metrics["brokers"].items():
#         for tp, tp_data in data.get("topic-partitions", {}).items():
#             batch_size_avg = tp_data.get("batch-size-avg", None)
#             record_send_rate = tp_data.get("record-send-rate", None)
#             if batch_size_avg and record_send_rate:
#                 logging.info(
#                     f"[Metrics] Broker: {node} | Partition: {tp} | "
#                     f"Avg Batch Size: {batch_size_avg:.0f} bytes | "
#                     f"Record Send Rate: {record_send_rate:.2f} rec/s"
#                 )
#     # Also log internal queue fill level
#     q_total = producer.metrics()["producer-metrics"].get("record-queue-time-avg", 0)
#     logging.info(f"[Producer Queue] Avg Queue Time: {q_total:.3f} ms")

# Ensure CSV header exists
if not os.path.exists(METRICS_FILE):
    with open(METRICS_FILE, mode='w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp", "broker", "topic_partition",
            "messages_sent", "throughput_msgs_per_sec",
            "avg_time_per_message_ms", "avg_batch_size_bytes",
            "record_send_rate_rec_s", "avg_queue_time_ms"])


# Log producer metrics to console and CSV
# def log_producer_metrics(producer):
#     """Periodically log Kafka producer metrics for tuning."""
#     metrics = producer.metrics()
#     timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

#     rows_to_write = []
#     for node, data in metrics.get("brokers", {}).items():
#         for tp, tp_data in data.get("topic-partitions", {}).items():
#             batch_size_avg = tp_data.get("batch-size-avg", None)
#             record_send_rate = tp_data.get("record-send-rate", None)
#             if batch_size_avg and record_send_rate:
#                 queue_time_avg = metrics["producer-metrics"].get("record-queue-time-avg", 0)
                
#                 # Log to console
#                 logging.info(
#                     f"[Metrics] Broker: {node} | Partition: {tp} | "
#                     f"Avg Batch Size: {batch_size_avg:.0f} bytes | "
#                     f"Record Send Rate: {record_send_rate:.2f} rec/s | "
#                     f"Avg Queue Time: {queue_time_avg:.3f} ms"
#                 )

#                 # Prepare row for CSV
#                 rows_to_write.append([
#                     timestamp, node, tp, f"{batch_size_avg:.0f}", 
#                     f"{record_send_rate:.2f}", f"{queue_time_avg:.3f}"
#                 ])
    
#     # Append to CSV
#     if rows_to_write:
#         with open(METRICS_FILE, mode='a', newline='') as f:
#             writer = csv.writer(f)
#             writer.writerows(rows_to_write)


# Manual metrics tracking
metrics_state = {
    "messages_sent": 0,
    "total_bytes": 0,
    "start_time": time.time()
}


def log_producer_metrics():
    """Manually log simplified Kafka producer metrics to CSV and console."""
    now = time.time()
    elapsed_time = now - metrics_state["start_time"]
    if elapsed_time <= 0:
        return

    throughput = metrics_state["messages_sent"] / elapsed_time
    avg_time_per_msg = (elapsed_time / metrics_state["messages_sent"] * 1000) if metrics_state["messages_sent"] > 0 else 0
    avg_batch_size = metrics_state["total_bytes"] / metrics_state["messages_sent"] if metrics_state["messages_sent"] > 0 else 0

    timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    logging.info(
        f"[Metrics] Total Sent: {metrics_state['messages_sent']} | "
        f"Throughput: {throughput:.2f} msg/s | "
        f"Avg Msg Time: {avg_time_per_msg:.2f} ms | "
        f"Avg Batch Size: {avg_batch_size:.0f} bytes"
    )

    # Append simplified row to CSV
    with open(METRICS_FILE, mode='a', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([
            timestamp, "-", "-", metrics_state["messages_sent"],
            f"{throughput:.2f}", f"{avg_time_per_msg:.2f}",
            f"{avg_batch_size:.0f}", "-", "-"
        ])

    # Reset counters
    metrics_state["messages_sent"] = 0
    metrics_state["total_bytes"] = 0
    metrics_state["start_time"] = now


# data serialization
def json_serializer(data):
    return json.dumps(data).encode('utf-8')


# to capture errors and successful message delivery
def delivery_report(err, msg):
    if err:
        logging.error(f"Message delivery failed: {err}")
    else:
        logging.info(f"Message delivered to {msg.topic()} [{msg.partition()}]")


#main producer logic
def stream_produce(poll_interval=POLL_INTERVAL):
    ensure_state_table()
    last_height = get_last_processed_height()

    if last_height:
        logger.info(f"Resuming from checkpoint block {last_height}")
    else:
        if START_HEIGHT:
            last_height = START_HEIGHT
            logger.info(f"Starting from configured block height {START_HEIGHT}")
        elif START_DATE:
            last_height = get_height_from_date(START_DATE)
            logger.info(f"Starting from date {START_DATE}, block height {last_height}")
        else:
            last_height = get_latest_height()
            logger.info(f"Starting from tip block {last_height}")
        update_last_processed_height(last_height)
    
    # Initialize timer for logging metrics
    last_metrics_time = time.time()

    try:
        while True:
            try:
                tip = get_latest_height()
            except Exception as e:
                logger.error(f"Failed to fetch latest height: {e}")
                time.sleep(poll_interval)
                continue

            if last_height < tip:
                for h in range(last_height + 1, tip + 1):
                    logger.info(f"Extracting block {h}")
                    try:
                        block_data, detailed_txs = extract_block(h)

                        if block_data:
                            #get length of data in bytes
                            msg_bytes = len(json.dumps(block_data).encode('utf-8'))
                            producer.produce(BLOCK_TOPIC,value=json_serializer(block_data),callback=delivery_report)
                            #get producer metrics
                            metrics_state["messages_sent"] += 1
                            metrics_state["total_bytes"] += msg_bytes
                            producer.poll(0)

                        if detailed_txs:
                            #get length of data in bytes
                            msg_bytes = len(json.dumps(detailed_txs).encode('utf-8'))
                            producer.produce(TRANSACTION_TOPIC,value=json_serializer(detailed_txs),callback=delivery_report)
                            #get producer metrics
                            metrics_state["messages_sent"] += 1
                            metrics_state["total_bytes"] += msg_bytes
                            producer.poll(0)

                        update_last_processed_height(h)

                        # Log producer metrics every 10 seconds
                        if time.time() - last_metrics_time >= 10:
                            log_producer_metrics()
                            last_metrics_time = time.time()

                    except Exception as e:
                        logger.error(f"Error processing block {h}: {e}")
                        time.sleep(5)
            else:
                logger.info("No new blocks. Waiting...")
                time.sleep(poll_interval)

    except KeyboardInterrupt:
        logger.info("Stopped by user.")
    

if __name__ == "__main__":
    try:
        stream_produce(poll_interval=POLL_INTERVAL)
    except KeyboardInterrupt:
        logger.info("Producer stopped by user")
    except Exception:
        logger.exception("Producer crashed")
        raise


#https://www.bing.com/search?q=confluent+kafka+producer+configurations&cvid=be2d8a8866184943a2350c14ee68de13&gs_lcrp=EgRlZGdlKgYIABBFGDkyBggAEEUYOTIGCAEQABhAMgYIAhAAGEAyBggDEAAYQDIGCAQQABhAMgYIBRAAGEAyBggGEAAYQDIGCAcQABhAMgYICBAAGEDSAQkxODQ4MGowajSoAgiwAgE&FORM=ANAB01&PC=U531
#https://docs.confluent.io/platform/current/installation/configuration/producer-configs.html#linger-ms
