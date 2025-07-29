import pykka
from kafka import KafkaConsumer,KafkaProducer
import threading
import pandas as pd
from collections import deque
import json
import socketio
import numpy as np


class SignalProcessor(pykka.ThreadingActor):
    def __init__(self):
        super().__init__()
        self.sio = socketio.Client()
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
        self.producer = KafkaProducer(
            bootstrap_servers='localhost:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.signal_history = deque(maxlen=20) 
        self.category=None
        self.sio.connect('http://localhost:5000')
        
    def on_receive(self, message):
        signal_name= message.get('signal_name')
        time_recorded = float(message.get('time'))
        signal_value = message.get('value')
        caseid = message.get('caseid')
        if None in (signal_name, time_recorded, signal_value, caseid):
            return
        category = None
        if self.category is None:
            for cat, names in self.category_mapping.items():
                if signal_name in names:
                    category = cat
                    break
            if category is None:
                return 
            self.category = category
        
        # self.signal_history.append({'time':time_recorded,'signal_value':signal_value})    
        room = f"patient_{caseid}_{self.category}"
        data = {
            'payload':{'signal_name':signal_name,
            'signal_value':signal_value,
            'time_recorded':time_recorded,
            'category':self.category,
            'caseid':caseid},
            'room':room
        }
        try:
            self.sio.emit('room_data', data)
            # print(f"Emitted --{caseid} ---to room {room}",)
        except Exception as e:
            print(f"Socket send failed here: {e}")
        
        signal_threshold = self.thresholds[self.category]
        
        # Sanity check
        if signal_value is None or time_recorded is None:
            # print(f"[DROP] Missing signal/time: {signal_name} | {signal_value} | {time_recorded}")
            return

        if not (signal_threshold['clip_min'] <= signal_value <= signal_threshold['clip_max']):
            # print(f"[DROP] Unreasonable value {signal_value} for {signal_name}")
            return


        # Add to history
        self.signal_history.append(signal_value)

        # ----------------------
        # RANGE-BASED DETECTION
        # ----------------------
        lc = signal_threshold.get('low_crit')
        hc = signal_threshold.get('high_crit')

        lm = signal_threshold.get('low_mild')
        hm = signal_threshold.get('high_mild')
        if (lc is not None and signal_value < lc) or (hc is not None and signal_value > hc):
            self.raise_alert(caseid, signal_name, signal_value, time_recorded, reason='critical_range_violation')
            return
        elif (lm is not None and signal_value < lm) or (hm is not None and signal_value > hm):
            self.raise_alert(caseid, signal_name, signal_value, time_recorded, reason='mild_range_violation')

        # ----------------------
        # Z-SCORE DETECTION
        # ----------------------
        if len(self.signal_history) >= 5:
            arr = np.array(self.signal_history)
            mean = arr.mean()
            std = arr.std()
            if std != 0:
                z = abs((signal_value - mean) / std)
                if z > signal_threshold['z_score_threshold_high']:
                    self.raise_alert(caseid, signal_name, signal_value, time_recorded, reason=f'z_score_critical:{z:.2f}')
                elif z > signal_threshold['z_score_threshold_mild']:
                    self.raise_alert(caseid, signal_name, signal_value, time_recorded, reason=f'z_score_mild:{z:.2f}')

        
        
        
    def raise_alert(self,caseid,signal_name,signal_value,time_recorded,reason):
        topic = f"alert_patient_{caseid}"
        message = {'payload':{
                        'signal_value':signal_value,
                        'time_recorded':time_recorded,
                        'caseid' : caseid,
                        'category':self.category,
                        'reason':reason
                    }, 
                   'room':topic}
        
        self.sio.emit('alert',message)
        print(f"case id : {caseid} reasoning : {reason}")
        self.producer.send(topic,value=message)
        
        
    def on_stop(self):
        print("Stopping SignalProcessor...")
        
        # --- Close the Kafka Producer ---
        if self.producer:
            self.producer.flush()
            self.producer.close()

        # --- Disconnect the Socket.IO client ---
        if self.sio and self.sio.connected:
            self.sio.disconnect()
            
        print("SignalProcessor stopped.")



class SignalActor(pykka.ThreadingActor):
    def __init__(self, details, name):
        super().__init__()
        self.tid = details['tid']
        self.tname = details['tname'].replace('/', '_')
        self.caseid = details['caseid']
        self.topic = f"{self.caseid}_{self.tname}"
        print(self.topic)
        self.name = name
        self.running = True
        
        self.consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers='localhost:9092',
            auto_offset_reset='latest',
            group_id='signal-consumer-group',
            enable_auto_commit=True,
            value_deserializer=lambda x: x.decode('utf-8')
        )

    def on_start(self):
        self.processor = SignalProcessor.start()
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
                        
                        
                        result = self.processor.ask(processing_message)
                        # print(time_recorded , signal_name,sep="-----")
                    except Exception as e:
                        print(f"[{self.name}] Error: {e}")

    def on_stop(self):
        print(f"SignalActor {self.name} stopping...")
        self.running = False # Signal the consume_loop to stop
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=5) # Wait for the consumer thread to finish
            if self.thread.is_alive():
                print(f"[{self.name}] Warning: Consumer thread did not terminate gracefully.")
        self.consumer.close() 
        # Stop the child SignalProcessor actor
        if self.processor:
            pykka.ActorRegistry.stop(self.processor)
        
        print(f"SignalActor {self.name} stopped.")

signal_info_df = pd.read_excel('data_preparation/data/final_signals_info.xlsx')
class PatientActor(pykka.ThreadingActor):
    def __init__(self,case_id):
        super().__init__()
        self.caseid=case_id
        self.signal_actors = []
        self.name = f"patient-{self.caseid}-actor"
    def on_start(self):
        for _,row in signal_info_df[signal_info_df['caseid']==self.caseid].iterrows():
            tid = row['tid']
            tname = row['tname']
            name = f"{row['tname']}"
            actor = SignalActor.start(details=row,name = name)
            self.signal_actors.append({'ref': actor, 'actor_name': name, 'tid': tid, 'tname': tname})
    def on_receive(self, alert):
        print(self.name,alert,sep="\n")
    def on_stop(self):
        print(f"PatientActor {self.name} stopping...")
        for actor_info in self.signal_actors:
            pykka.ActorRegistry.stop(actor_info['ref'])
        print(f"PatientActor {self.name} stopped.")


import pykka
caseid_list = sorted([3638, 1209, 1087, 1903, 5540, 5211, 2422, 2327, 1952, 634, 4721, 4658, 6234, 2880, 2340, 1488, 3519, 2191, 2626, 5248, 1032, 5841, 4978, 6335, 2259, 5337, 3984, 2724, 609, 5107])
class SupervisorActor(pykka.ThreadingActor):
    def __init__(self):
        super().__init__()
        self.actors_list = []
    def on_start(self):
        for i in range(len(caseid_list)):
            name=f"patient-{caseid_list[i]}-actor"
            patient_actor = PatientActor.start(case_id = caseid_list[i])
            self.actors_list.append({'patient_ref':patient_actor,'actor_name':name})
    def on_receive(self,message):
        print(message.get('alert'))
    
    def on_stop(self):
        print("SupervisorActor stopping...")
        for actor_info in self.actors_list:
            pykka.ActorRegistry.stop(actor_info['patient_ref'])
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
        if supervisor:
            # Stop the supervisor actor, which in turn stops all child actors
            pykka.ActorRegistry.stop_all()
            print("All actors stopped. Pykka shutdown initiated.")
        # This will wait for all actors to shut down
        # pykka.ActorRegistry.wait_until_stopped() # Wait for all Pykka actors to fully stop
        print("Pykka ActorRegistry : ")
        print(pykka.ActorRegistry.get_all())