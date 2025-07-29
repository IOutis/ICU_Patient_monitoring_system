import pandas as pd
import pykka
from kafka import KafkaProducer
import json
import time
import threading

class ProducerSignalActor(pykka.ThreadingActor):
    def __init__(self, details):
        super().__init__()
        self.running = True
        self.thread = None
        self.tid = details['tid']
        self.caseid = details['caseid']
        self.tname = details['tname'].replace('/', '_')
        self.path = f"data_preparation/tracks/track_data_{self.tid}_{self.tname}.csv"
        self.topic = f"{self.caseid}_{self.tname}"
        
        try:
            self.data = pd.read_csv(self.path)
        except FileNotFoundError:
            print(f"WARNING: Data file not found: {self.path}. Actor for topic {self.topic} will not produce data.")
            self.data = pd.DataFrame() # Use an empty DataFrame to prevent errors

        self.producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda m: json.dumps(m).encode('ascii')
        )

    def _produce_loop(self):
        """This method runs in a separate thread to send data."""
        print(f"Producer loop started for topic: {self.topic}")
        for _, row in self.data.iterrows():
            # Check the flag at the start of each iteration
            if not self.running:
                break
            
            signal_row_dict = row.to_dict()
            self.producer.send(topic=self.topic, value=signal_row_dict)
            print(f"[{self.topic}] Sent: {signal_row_dict}")
            # Use a non-blocking sleep that can be interrupted more easily
            time.sleep(0.5)
        
        print(f"Producer loop finished for topic: {self.topic}")

    def on_start(self):
        """Starts the background thread for producing messages."""
        self.thread = threading.Thread(target=self._produce_loop)
        self.thread.start()

    def on_stop(self):
        """Gracefully stops the producer loop and closes resources."""
        print(f"Stopping Producer for topic: {self.topic}")
        self.running = False  # Signal the loop to terminate
        
        if self.thread:
            self.thread.join(timeout=2.0) # Wait for the thread to finish
        
        self.producer.flush()
        self.producer.close()
        print(f"Producer for topic {self.topic} is closed.")

            

if __name__ == "__main__":
    signal_info_df = pd.read_excel('data_preparation/data/final_signals_info.xlsx')
    
    try:
        # Start all producer actors
        for _, row in signal_info_df.iterrows():
            ProducerSignalActor.start(row.to_dict())
        
        print(f"All producers are running. Press Ctrl+C to stop.")
        
        # Keep the main thread alive to catch the interrupt
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\nKeyboardInterrupt received. Initiating graceful shutdown...")
        
    finally:
        # This sends a stop message to all running actors and waits for them to finish
        pykka.ActorRegistry.stop_all(block=True, timeout=5)
        print("All producers have been stopped. Application exiting.")