from confluent_kafka import Consumer, KafkaException, KafkaError, Producer
import configparser
from pymongo import MongoClient
import json
import time
import threading

# generate config from kafka_config.ini
def read_config (config_env):
    config = configparser.ConfigParser()
    config.read('kafka_config.ini')
    config_dict = {}
    for k,v in config[config_env].items():
        config_dict[k] = v
    return config_dict
   

# Function to read from Kafka and write to MongoDB
def process_kafka_data(REMOTE_KAFKA_CONFIG, LOCAL_KAFKA_CONFIG, REMOTE_TOPIC,LOCAL_TOPIC,MONGODB_URI,DB_NAME,COLLECTION_NAME):


    # Initialize Kafka Consumer
    consumer = Consumer(REMOTE_KAFKA_CONFIG)
    consumer.subscribe([REMOTE_TOPIC])

    # Initialize Kafka Producer
    producer = Producer(LOCAL_KAFKA_CONFIG)

    # Initialize MongoDB Client
    mongo_client = MongoClient(MONGODB_URI)
    db = mongo_client[DB_NAME]
    collection = db[COLLECTION_NAME]
    count = 5

    try:
        while count > 0:
            # Poll for messages from the remote Kafka topic
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            # Decode the message value
            message_value = msg.value().decode('utf-8')
            print(f"Received message: {message_value}")

            # Produce the message to the local Kafka topic
            producer.produce(LOCAL_TOPIC, message_value)
            

            # Insert the message into MongoDB
            try:
                message_data = json.loads(message_value)
                collection.insert_one(message_data)
                print(f"Inserted message into MongoDB: {message_data}")
            except Exception as e:
                print(f"Failed to insert message into MongoDB: {e}")
            
            
            count = count - 1

    except KeyboardInterrupt:
        print("Process interrupted by user")

    finally:
        # Cleanup
        producer.flush()
        consumer.close()
        mongo_client.close()
        
        
# Run the function
if __name__ == "__main__":

    #define kafka topics
    kafka_topic_remote = 'product_view'
    kafka_topic_local = 'local_topic'

    # Read config from file
    kafka_remote = read_config('KAFKA_PROD_SERVER')
    kafka_local = read_config('KAFKA_DEV_SERVER')
    mongodb_local = read_config('MONGO_DB')
    
    # Assign mongodb variables
    mongodb_uri = mongodb_local['mongodb_uri']
    mongodb_db_name = mongodb_local['db_name']
    mongodb_collection_name = mongodb_local['collection_name']

                   
    # Define consumer in consumer-group

    thread1 = threading.Thread(target=process_kafka_data, args=(kafka_remote,kafka_local,kafka_topic_remote,kafka_topic_local,mongodb_uri,mongodb_db_name,mongodb_collection_name))
    thread2 = threading.Thread(target=process_kafka_data, args=(kafka_remote,kafka_local,kafka_topic_remote,kafka_topic_local,mongodb_uri,mongodb_db_name,mongodb_collection_name))

    # Start multi-threading
    thread1.start()
    thread2.start()
    

    # Join threads
    thread1.join()
    thread2.join()
    
    print('DONEEEEEEEEEEEEEEEEE')
