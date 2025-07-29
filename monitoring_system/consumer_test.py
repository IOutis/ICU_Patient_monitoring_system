import sys
import os
from producer_simulation import send_message,producer
import json
from kafka import KafkaConsumer
import threading
import time


# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
# root = os.path.abspath(os.path.join(__file__, '../../'))
consumer = KafkaConsumer('my_topic',bootstrap_servers=['localhost:9092'],value_deserializer=lambda m: json.loads(m.decode('ascii')))
def consume_messages():
    print("Consumer thread started. Waiting for messages...")
    try:
        for message in consumer:
            print(f"Received message: {message.value}")
    except KeyboardInterrupt:
        print("Consumer thread stopped.")
    finally:
        consumer.close()
        
if __name__ == "__main__":
    # Start the consumer in a separate thread
    consumer_thread = threading.Thread(target=consume_messages)
    consumer_thread.daemon = True # Allows main program to exit even if thread is running
    consumer_thread.start()

    # Give the consumer a moment to subscribe and be ready
    time.sleep(2)

    # Send messages after the consumer is potentially ready
    print("Sending messages from main thread...")
    send_message({"greeting": "Hello from combined script!"})
    send_message({"intro": "I am Mushtaq from combined script!"})

    # Keep the main thread alive for a bit to allow messages to be consumed
    print("Messages sent. Keeping script alive for 10 seconds to consume...")
    time.sleep(10)
    print("Exiting.")
    producer.close()