import pykka
from kafka import KafkaConsumer,KafkaProducer
import threading
import pandas as pd
from collections import deque
import json
import numpy as np
import os
from monitoring_system.access_socket import connect,get_sio
from kafka.admin import KafkaAdminClient,NewTopic


import socketio


producer = KafkaProducer(
            bootstrap_servers='localhost:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )


class CentralKafkaConsumer(threading.Thread):
    def __init__(self, patient_actors_map: dict):
        super().__init__(daemon=True)
        self.patient_actors = patient_actors_map
        self.consumer = KafkaConsumer(
            bootstrap_servers='localhost:9092',
            group_id='patient-alert-router-group',
            auto_offset_reset='latest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        # Subscribe to all alert topics using a wildcard pattern
        self.consumer.subscribe(pattern='^alert_patient_.*')
        self.running = True

    def run(self):
        # logger.info("✅ Central Kafka Consumer started...")
        for message in self.consumer:
            if not self.running:
                break
            
            alert_payload = message.value.get('payload', {})
            case_id = alert_payload.get('caseid')
            
            if case_id:
                patient_actor_ref = self.patient_actors.get(f"patient_{case_id}")
                if patient_actor_ref:
                    # Use .tell() to route the alert to the correct PatientActor
                    patient_actor_ref.tell(alert_payload)

    def stop(self):
        self.running = False
        self.consumer.close()
        # logger.info("Central Kafka Consumer stopped.")
        
        




class SignalProcessor(pykka.ThreadingActor):
    def __init__(self,parent_ref:pykka.ActorRef=None):
        super().__init__()
        self.sio = socketio.Client()

        self.parent_ref = parent_ref
        self.thresholds={
            "HR": {
        "low_mild": 60, "high_mild": 100,
        "low_crit": 40, "high_crit": 140,
        "z_score_threshold_mild": 2.0, "z_score_threshold_high": 3.0,
        "clip_min": 30, "clip_max": 200 # For sanity checking incoming data
        },
        "SpO2": {
            "low_mild": 90, "high_mild": None, # SpO2 typically drops, not spikes high
            "low_crit": 85, "high_crit": None,
            "z_score_threshold_mild": 2.0, "z_score_threshold_high": 3.0,
            "clip_min": 70, "clip_max": 100
        },
        "ART_MBP": {
            "low_mild": 65, "high_mild": 105,
            "low_crit": 55, "high_crit": 120,
            "z_score_threshold_mild": 2.0, "z_score_threshold_high": 3.0,
            "clip_min": 50, "clip_max": 140
        },
        "RR": {
            "low_mild": 10, "high_mild": 24,
            "low_crit": 8, "high_crit": 30,
            "z_score_threshold_mild": 2.0, "z_score_threshold_high": 3.0,
            "clip_min": 5, "clip_max": 40
        },
        "BT": {
            "low_mild": 36.0, "high_mild": 38.0,
            "low_crit": 35.0, "high_crit": 39.0,
            "z_score_threshold_mild": 2.0, "z_score_threshold_high": 3.0,
            "clip_min": 34.0, "clip_max": 40.0
        },
        "ETCO2": {
            "low_mild": 30, "high_mild": 50,
            "low_crit": 25, "high_crit": 60,
            "z_score_threshold_mild": 2.0, "z_score_threshold_high": 3.0,
            "clip_min": 20, "clip_max": 70
        },
        "CVP": {
            "low_mild": 2, "high_mild": 8,
            "low_crit": 0, "high_crit": 12,
            "z_score_threshold_mild": 2.0, "z_score_threshold_high": 3.0,
            "clip_min": -5, "clip_max": 15
        },
        "ECG_II": {
            "low_mild": None, "high_mild": None, # Placeholder: ECG needs specialized anomaly detection
            "low_crit": None, "high_crit": None,
            "z_score_threshold_mild": None, "z_score_threshold_high": None,
            "clip_min": -5, "clip_max": 5 # Example clip for a generic voltage sample
        },
        "ART_SBP": { # Arterial Systolic Blood Pressure
        "low_mild": 90, "high_mild": 140,
        "low_crit": 70, "high_crit": 160,
        "z_score_threshold_mild": 2.0, "z_score_threshold_high": 3.0,
        "clip_min": 50, "clip_max": 200
        },
        "ART_DBP": { # Arterial Diastolic Blood Pressure
            "low_mild": 60, "high_mild": 90,
            "low_crit": 40, "high_crit": 100,
            "z_score_threshold_mild": 2.0, "z_score_threshold_high": 3.0,
            "clip_min": 30, "clip_max": 120
        },
        }
        
        self.category_mapping={
            'HR':[
                    "Solar8000/HR",
                    "CardioQ/HR",
                    "Solar8000/PLETH_HR"
                ],
            "SpO2":["Solar8000/PLETH_SPO2"],
            "ART_SBP":["Solar8000/ART_SBP"],
            "ART_DBP":["Solar8000/ART_DBP"],
            "ART_MBP":[
                "Solar8000/ART_MBP",
                "EV1000/ART_MBP"],
            "RR":[
                    "Solar8000/RR",
                    "Solar8000/RR_CO2",
                    "Primus/RR_CO2",
                    "Solar8000/VENT_RR"
                ],
            "BT": ["Solar8000/BT"],
            "ETCO2": [
                "Solar8000/ETCO2",
                "Primus/ETCO2"
                ],
            "CVP": ["SNUADC/CVP",
                    "Solar8000/CVP",
                    "EV1000/CVP"
                    ],
            "ECG_II":
                [
                    "SNUADC/ECG_II"
                ]
        }
        self.sio.connect('http://localhost:5000')
        
        self.signal_history = deque(maxlen=20) 
        self.category=None
        
        
    def on_receive(self, message):
        try:
            signal_name = message.get('signal_name')
            time_recorded = message.get('time')
            signal_value = message.get('value')
            caseid = message.get('caseid')
            
            # Enhanced validation
            if None in (signal_name, time_recorded, signal_value, caseid):
                return
                
            # Convert time to float if it's a string
            try:
                if isinstance(time_recorded, str):
                    time_recorded = float(time_recorded)
            except (ValueError, TypeError):
                return
                
            # Additional validation for signal_value
            if isinstance(signal_value, str):
                if signal_value.strip() == '' or signal_value.lower() in ['null', 'nan', 'none']:
                    return
                try:
                    signal_value = float(signal_value)
                except (ValueError, TypeError):
                    return
            
            # Check if signal_value is a valid number
            if not isinstance(signal_value, (int, float)) or np.isnan(signal_value) or np.isinf(signal_value):
                return
            
            # Determine category
            category = None
            if self.category is None:
                for cat, names in self.category_mapping.items():
                    if signal_name in names:
                        category = cat
                        break
                if category is None:
                    return
                self.category = category
            
            # Emit to SocketIO (with error handling)
            room = f"patient_{caseid}_{self.category}"
            data = {
                'payload': {
                    'signal_name': signal_name,
                    'signal_value': signal_value,
                    'time_recorded': time_recorded,
                    'category': self.category,
                    'caseid': caseid
                },
                'room': room
            }
            
            try:
                if self.sio and self.sio.connected:
                    self.sio.emit('room_data', data)
            except Exception as e:
                print(f"Socket send failed: {e}")
            
            # Get thresholds for this category
            signal_threshold = self.thresholds.get(self.category, {})
            if not signal_threshold:
                return
            
            # Sanity check
            clip_min = signal_threshold.get('clip_min')
            clip_max = signal_threshold.get('clip_max')
            if clip_min is not None and clip_max is not None:
                if not (clip_min <= signal_value <= clip_max):
                    return
            
            # Add to history
            self.signal_history.append(signal_value)
            
            # Range-based detection
            lc = signal_threshold.get('low_crit')
            hc = signal_threshold.get('high_crit')
            lm = signal_threshold.get('low_mild')
            hm = signal_threshold.get('high_mild')
            
            if (lc is not None and signal_value < lc) or (hc is not None and signal_value > hc):
                self.raise_alert(caseid, signal_name, signal_value, time_recorded, reason='critical_range_violation')
                return
            elif (lm is not None and signal_value < lm) or (hm is not None and signal_value > hm):
                self.raise_alert(caseid, signal_name, signal_value, time_recorded, reason='mild_range_violation')
            
            # Z-score detection
            if len(self.signal_history) >= 5:
                arr = np.array(self.signal_history)
                mean = arr.mean()
                std = arr.std()
                if std != 0:
                    z = abs((signal_value - mean) / std)
                    z_threshold_high = signal_threshold.get('z_score_threshold_high')
                    z_threshold_mild = signal_threshold.get('z_score_threshold_mild')
                    
                    if z_threshold_high is not None and z > z_threshold_high:
                        self.raise_alert(caseid, signal_name, signal_value, time_recorded, reason=f'z_score_critical:{z:.2f}')
                    elif z_threshold_mild is not None and z > z_threshold_mild:
                        self.raise_alert(caseid, signal_name, signal_value, time_recorded, reason=f'z_score_mild:{z:.2f}')
            
            # Flatline detection
            FLATLINE_COUNT = 10
            if len(self.signal_history) >= FLATLINE_COUNT:
                last_values = list(self.signal_history)[-FLATLINE_COUNT:]
                if all(v == last_values[0] for v in last_values):
                    self.raise_alert(caseid, signal_name, signal_value, time_recorded, reason='flatline_detected')
                    
        except Exception as e:
            print(f"Error in SignalProcessor.on_receive: {e}")
        
    def raise_alert(self, caseid, signal_name, signal_value, time_recorded, reason):
        try:
            topic = f"alert_patient_{caseid}"
            message = {
                'payload': {
                    'signal_value': signal_value,
                    'time_recorded': time_recorded,
                    'caseid': caseid,
                    'category': self.category,
                    'reason': reason
                },
                'room': topic
            }
            
            # Emit to SocketIO
            try:
                if self.sio and self.sio.connected:
                    self.sio.emit('alert', message)
            except Exception as e:
                print(f"[ERROR] Failed to emit alert: {e}")
            
            # Send to Kafka
            producer.send(topic, value=message)
            
            # Tell parent
            if self.parent_ref:
                self.parent_ref.tell(message["payload"])
                
        except Exception as e:
            print(f"Error in raise_alert: {e}")
        
    def on_stop(self):
        print("Stopping SignalProcessor...")
        if self.sio and self.sio.connected:
            try:
                self.sio.disconnect()
            except Exception as e:
                print(f"Error disconnecting SocketIO: {e}")
        print("SignalProcessor stopped.")



class SignalActor(pykka.ThreadingActor):
    def __init__(self, details, name,parent_ref:pykka.ActorRef=None):
        super().__init__()
        self.tid = details['tid']
        self.tname = details['tname'].replace('/', '_')
        self.caseid = details['caseid']
        self.topic = f"{self.caseid}_{self.tname}"
        # print(self.topic)
        self.name = name
        self.running = True
        self.parent_ref = parent_ref
        self.processor=None
        
        self.consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers='localhost:9092',
            auto_offset_reset='latest',
            group_id='signal-consumer-group',
            enable_auto_commit=True,
            value_deserializer=lambda x: x.decode('utf-8')
        )

    def on_start(self):
        self.processor = SignalProcessor.start(self.parent_ref)
        self.thread = threading.Thread(target=self.consume_loop, daemon=True)
        self.thread.start()

    def consume_loop(self):
        while self.running:
            messages = self.consumer.poll(timeout_ms=1000)
            for tp, msgs in messages.items():
                for msg in msgs:
                    try:
                        received_message = json.loads(msg.value)
                        keys = list(received_message.keys())
                        signal_name = keys[1]
                        values = list(received_message.values())
                        time_recorded = values[0]
                        signal_value = values[1]
                        processing_message = {
                            'signal_name':signal_name,
                            'time':time_recorded,
                            'value':signal_value,
                            'caseid':self.caseid
                        }
                        
                        
                        if self.processor and self.processor.is_alive():
                            self.processor.tell(processing_message)
                        else:
                            print(f"[{self.name}] Processor not available, skipping message")
                            break 
                        
                        # print(time_recorded , signal_name,sep="-----")
                    except Exception as e:
                        # logger.warning(f"[{self.name}] Error: {e}")
                        print(f"[{self.name}] {self.caseid} Error: {e}")

    def on_stop(self):
        # logger.info(f"SignalActor {self.name} stopping...")
        self.running = False # Signal the consume_loop to stop
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=5) # Wait for the consumer thread to finish
            if self.thread.is_alive():
                # logger.warning(f"[{self.name}] Warning: Consumer thread did not terminate gracefully.")
                print(f"[{self.name}] Warning: Consumer thread did not terminate gracefully.")
        self.consumer.close() 
        # Stop the child SignalProcessor actor
        if self.processor:
            # pykka.ActorRegistry.stop_all(self.processor)
            self.processor.stop()
        
        # logger.info(f"SignalActor {self.name} stopped.")

from queue import Queue
signal_info_df = pd.read_excel('data_preparation/data/final_signals_info.xlsx')
class PatientActor(pykka.ThreadingActor):
    def __init__(self,case_id):
        super().__init__()
        self.caseid=case_id
        self.signal_actors = []
        self.name = f"patient-{self.caseid}-actor"
        self.alert_topic = f"alert_patient_{self.caseid}"
        self.alert_aggregators=[]
        self.cooldown = 5
        
        # State for alert aggregation
        self.alert_buffer = Queue(maxsize=20)
        self.dispatch_task = None
        self.dispatch_timer = None
        self.timer_lock = threading.Lock()
    def on_start(self):
        # alert_actor = AlertAggregatorActor.start()
        for _,row in signal_info_df[signal_info_df['caseid']==self.caseid].iterrows():
            tid = row['tid']
            tname = row['tname']
            name = f"{row['tname']}"
            actor = SignalActor.start(details=row,name = name,parent_ref=self.actor_ref)
            self.signal_actors.append({'ref': actor, 'actor_name': name, 'tid': tid, 'tname': tname})
            
            
        self.ensure_kafka_topic_exists(self.alert_topic)
        
        
    def ensure_kafka_topic_exists(self,topic_name):
        admin_client = KafkaAdminClient(bootstrap_servers="localhost:9092")
        topic_list = admin_client.list_topics()
        if topic_name not in topic_list:
            admin_client.create_topics([NewTopic(name=topic_name, num_partitions=1, replication_factor=1)])
            # logger.info(f"[PatientActor] Created topic: {topic_name}")
        admin_client.close()    
        
    
    def on_receive(self, alert_message):
        try:
            # CHANGE: Using thread-safe Queue instead of list
            self.alert_buffer.put(alert_message)

            # CHANGE: Fixed async/threading issue - using threading.Timer instead of asyncio
            with self.timer_lock:
                if self.dispatch_timer is None or not self.dispatch_timer.is_alive():
                    self.dispatch_timer = threading.Timer(self.cooldown, self._process_batch)
                    self.dispatch_timer.start()
                    
        except Exception as e:
            # logger.error(f"Error receiving alert in PatientActor {self.name}: {e}")
            print(f"Error receiving alert in PatientActor {self.name}: {e}")

    def _process_batch(self):
        """CHANGE: Replaced async method with synchronous threading approach"""
        try:
            # Collect all alerts from the queue
            
            batch_to_process = []
            while not self.alert_buffer.empty():
                try:
                    alert = self.alert_buffer.get_nowait()
                    batch_to_process.append(alert)
                except:
                    break  # Queue is empty

            if batch_to_process:
                print(f"Patient-{self.caseid}: Processing batch of {len(batch_to_process)} alerts")
                self.push_llm_kafka(batch_to_process)
            
            # Reset timer reference
            with self.timer_lock:
                self.dispatch_timer = None
                
        except Exception as e:
            print(f"Error processing alert batch for patient {self.caseid}: {e}")

            
    def push_llm_kafka(self, alert_batch: list):
        """This is where you will implement your LLM agent logic."""
        groups = {}
        for alert in alert_batch:
            category = alert['category']
            if category not in groups:
                groups[category] = []
            groups[category].append(alert)
        for category in groups:
            groups[category].sort(key=lambda alert: alert['time_recorded'])
        result = []
        for category, alerts in groups.items():
            group_info = {
                'category': category,
                'alerts': alerts,
                'count': len(alerts)
            }
            result.append(group_info)
        print(f"-> Agent for patient {self.caseid} would now analyze {len(result)}")
        topic_name = f"llm_alert_patient_{self.caseid}"
        try:
            producer.send(topic =topic_name, value =result)
        except Exception as e:
            print("error here in pushing to kafka : ",e)
    
    
    def on_stop(self):
        # logger.info(f"PatientActor {self.name} stopping...")
        for actor_info in self.signal_actors:
            pykka.ActorRegistry.stop(actor_info['ref'])
        # logger.info(f"PatientActor {self.name} stopped.")


import pykka
caseid_list = [609, 634, 1032, 1087, 1209, 1488, 1903, 1952, 2191, 2259, 2327, 2340, 2422, 2626, 2724, 2880, 3519, 3638, 3984, 4658, 4721, 4978, 5107, 5211, 5248, 5337, 5540, 5841, 6234, 6335]
class SupervisorActor(pykka.ThreadingActor):
    def __init__(self):
        super().__init__()
        self.patient_actors_map = {}
        self.kafka_consumer_thread = None
    def on_start(self):
        for case_id in caseid_list:
            patient_ref = PatientActor.start(case_id=case_id)
            self.patient_actors_map[f"patient_{case_id}"] = patient_ref
            
        self.kafka_consumer_thread = CentralKafkaConsumer(self.patient_actors_map)
        self.kafka_consumer_thread.start()
        # logger.info("✅ Supervisor started all PatientActors and the Central Kafka Consumer.")
        
        
    def on_receive(self,message):
        pass
        # logger.info(message.get('alert'))
    
    def on_stop(self):
        print("SupervisorActor stopping...")
        if self.kafka_consumer_thread:
            self.kafka_consumer_thread.stop()
            self.kafka_consumer_thread.join()
        for actor_info in self.patient_actors_map:
            pykka.ActorRegistry.stop(actor_info)
        print("SupervisorActor stopped.")

        
        





import time
import os
if __name__ == "__main__":
    # print(os.getcwd())
    supervisor = None
    try:
        supervisor = SupervisorActor.start()
        print("Application running. Press Ctrl+C to stop.")
        while True:
            time.sleep(1) # Sleep to prevent busy-waiting
    except KeyboardInterrupt:
        print("\nKeyboardInterrupt received. Initiating graceful shutdown...")
    finally:
        if producer:
            producer.flush()
            producer.close()

        if supervisor:
            # Stop the supervisor actor, which in turn stops all child actors
            pykka.ActorRegistry.stop_all()
            print("All actors stopped. Pykka shutdown initiated.")
        # This will wait for all actors to shut down
        # pykka.ActorRegistry.wait_until_stopped() # Wait for all Pykka actors to fully stop
        print("Pykka ActorRegistry : ")
        print(pykka.ActorRegistry.get_all())