from confluent_kafka.admin import AdminClient, NewTopic

# Kafka bootstrap servers
BOOTSTRAP_SERVERS = "localhost:9092"

# Define topic name
TRANSACTION_TOPIC = "bitcoin_transaction_data"

# Create an AdminClient instance
admin_client = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})

#check topics in list of topics and save in variable
existing_topics = admin_client.list_topics(timeout=5).topics

# Check if the topic already exists
if TRANSACTION_TOPIC in existing_topics:
    print(f"Topic '{TRANSACTION_TOPIC}' already exists.")
else:
    # Define the topic with its properties
    topic1 = NewTopic(TRANSACTION_TOPIC, num_partitions=5, replication_factor=1)

    #create thetopic in kafka by sending topic creation request
    create_topic = admin_client.create_topics([topic1])

    # check topic creation
    for topic, f in create_topic.items():
        try:
            f.result()  # Block until the topic is created
            print(f"Topic '{topic}' created successfully!") #gets printed if topic successfully created
        except Exception as e:
            print(f"Failed to create topic '{topic}': {e}") #the error message gets printed if topic fails to get created
