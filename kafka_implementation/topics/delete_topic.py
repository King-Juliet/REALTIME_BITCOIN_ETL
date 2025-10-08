import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from confluent_kafka.admin import AdminClient

# Kafka bootstrap servers
BOOTSTRAP_SERVERS = "localhost:9092"

# Define topic name to delete
TOPIC_TO_DELETE = "topic_to_delete"  # Change this to the topic you want to delete


# Create an AdminClient instance
admin_client = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})

# Get the list of existing topics
existing_topics = admin_client.list_topics(timeout=5).topics

# Check if topic exists before trying to delete
if TOPIC_TO_DELETE not in existing_topics:
    print(f"Topic '{TOPIC_TO_DELETE}' does not exist. Nothing to delete.")
else:
    # Send delete request
    delete_topic = admin_client.delete_topics([TOPIC_TO_DELETE])

    # Verify deletion
    for topic, f in delete_topic.items():
        try:
            f.result()  # Block until the topic deletion is complete
            print(f"Topic '{topic}' deleted successfully!")
        except Exception as e:
            print(f"Failed to delete topic '{topic}': {e}")
