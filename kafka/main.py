from confluent_kafka import Consumer, KafkaException, KafkaError

def read_from_kafka():
    # Kafka configuration
    kafka_config = {
        'bootstrap.servers': '113.160.15.232:9094,113.160.15.232:9194,113.160.15.232:9294',
        'security.protocol': 'SASL_PLAINTEXT',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': 'kafka',
        'sasl.password': 'UnigapKafka@2024',
        'group.id': 'product_view_group',  # Consumer group ID
        'auto.offset.reset': 'earliest',   # Start reading at the earliest message
        'enable.auto.commit': True         # Automatically commit offsets
    }

    # Topic to read from
    topic = 'product_view'

    # Create Kafka Consumer
    consumer = Consumer(kafka_config)

    # Subscribe to the topic
    consumer.subscribe([topic])
    count = 5
    try:
        print(f"Listening to Kafka topic: {topic}")
        with open('output.txt','w') as file:
            while count > 0:
                msg = consumer.poll(1.0)  # Poll every 1 second
                
                   
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        print(f"Reached end of partition: {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
                    elif msg.error():
                        raise KafkaException(msg.error())
                else:
                    # Message successfully received
                    print(f"Received message: {msg.value().decode('utf-8')}")

                    # Write message to file
                    file.write(msg.value().decode('utf-8') + "\n")
                    file.flush()  # Ensure data is written immediately
                    count = count - 1

    except KeyboardInterrupt:
        print("Shutting down consumer...")

    finally:
        # Clean up resources
        consumer.close()

# Example usage
if __name__ == "__main__":
    read_from_kafka()
