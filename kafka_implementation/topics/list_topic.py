import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from confluent_kafka.admin import AdminClient

BOOTSTRAP_SERVERS = "localhost:9092"

admin_client = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})

# List existing topics
existing_topics = admin_client.list_topics(timeout=5).topics
print("Existing topics:", existing_topics)
