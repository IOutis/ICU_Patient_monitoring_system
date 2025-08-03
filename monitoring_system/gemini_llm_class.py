import pykka
from threading import Thread
import threading
import time
import json
import queue
import redis
from kafka import KafkaProducer
from langchain.chat_models import init_chat_model
from langchain_core.prompts import ChatPromptTemplate
import os
from monitoring_system.groq_llm_class import create_kafka_consumer, GEMINI_API, GEMINI_API_2, caseids_list
import socketio
# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Initialize Redis connection
redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

# Hardcoded doctor list
AVAILABLE_DOCTORS = [
    "dr_smith_cardiology",
    "dr_jones_emergency", 
    "dr_williams_icu",
    "dr_brown_general",
    "dr_davis_pulmonology",
    "dr_taylor_neurology",
    "dr_wilson_surgery",
    "dr_garcia_internal_medicine",
    "dr_martinez_critical_care",
    "dr_anderson_anesthesiology"
]

class GeminiAgent(pykka.ThreadingActor):
    def __init__(self, agent_id, api_key, patient_list):
        super().__init__()
        self.sio = socketio.Client()
        self.agent_id = agent_id
        self.api_key = api_key
        self.patient_list = patient_list
        self.running = False
        self.processing_queue = queue.Queue(maxsize=100)
        self.consumer_threads = []
        self.worker_thread = None
        
        # Initialize Gemini LLM
        try:
            self.llm = init_chat_model(
                "gemini-2.0-flash-exp",
                model_provider="google_genai",
                api_key=self.api_key,
                temperature=0.1
            )
            print(f"✅ {self.agent_id}: Gemini LLM initialized")
        except Exception as e:
            print(f"❌ {self.agent_id}: Failed to initialize Gemini: {e}")
            self.llm = None
        
        # Decision prompt for single call
        self.decision_prompt = ChatPromptTemplate.from_messages([
            ("system", """You are a medical decision AI for ICU patient monitoring.

Your task: Analyze the medical alert and make TWO decisions:
1. PRIORITY LEVEL: urgent, high, moderate, negligible
2. DOCTOR ASSIGNMENT: Choose from available doctors

PRIORITY GUIDELINES:
- URGENT: Life-threatening (flatline, cardiac arrest, severe sepsis)
- HIGH: Serious concern needing prompt attention (abnormal vitals, deteriorating)  
- MODERATE: Notable abnormalities requiring monitoring
- NEGLIGIBLE: Minor variations, stable condition

DOCTOR SPECIALTIES:
- dr_smith_cardiology: Heart conditions, arrhythmias
- dr_jones_emergency: Urgent/critical cases
- dr_williams_icu: ICU management, ventilator
- dr_brown_general: General medicine, stable patients
- dr_davis_pulmonology: Respiratory issues
- dr_taylor_neurology: Neurological conditions
- dr_wilson_surgery: Surgical interventions
- dr_garcia_internal_medicine: Complex medical cases
- dr_martinez_critical_care: Critical/unstable patients
- dr_anderson_anesthesiology: Sedation, pain management

AVAILABLE DOCTORS: {available_doctors}

RESPOND IN EXACT JSON FORMAT:
{{
    "priority": "urgent|high|moderate|negligible",
    "assigned_doctor": "doctor_name_from_available_list",
    "reasoning": "Brief clinical reasoning for priority and doctor choice"
}}"""),
            ("user", "Patient ID: {case_id}\n\nMedical Alert from Groq Analysis:\n{alert_content}")
        ])

    def on_start(self):
        self.sio.connect('http://localhost:5000')
        self.running = True
        
        # Initialize Redis doctor availability (only once across all agents)
        self.initialize_redis_doctors()
        
        # Start consumer threads for each patient
        for case_id in self.patient_list:
            consumer_thread = Thread(
                target=self.consume_patient_alerts,
                args=(case_id,),
                daemon=True,
                name=f"{self.agent_id}_Consumer_{case_id}"
            )
            consumer_thread.start()
            self.consumer_threads.append(consumer_thread)
        
        # Start worker thread
        self.worker_thread = Thread(
            target=self.worker_loop,
            daemon=True,
            name=f"{self.agent_id}_Worker"
        )
        self.worker_thread.start()
        
        print(f"🧠 {self.agent_id}: Started monitoring {len(self.patient_list)} patients")

    def initialize_redis_doctors(self):
        """Initialize available doctors in Redis (thread-safe)"""
        try:
            # Check if already initialized
            if not redis_client.exists("available_doctors"):
                # Set available doctors
                redis_client.sadd("available_doctors", *AVAILABLE_DOCTORS)
                print(f"🏥 {self.agent_id}: Initialized {len(AVAILABLE_DOCTORS)} doctors in Redis")
            else:
                available_count = redis_client.scard("available_doctors")
                print(f"🏥 {self.agent_id}: Found {available_count} available doctors in Redis")
        except Exception as e:
            print(f"❌ {self.agent_id}: Redis initialization error: {e}")

    def consume_patient_alerts(self, case_id):
        """Consumer thread for specific patient"""
        topic = f"llm_generated_patient_{case_id}"
        
        try:
            consumer = create_kafka_consumer(topic)
            print(f"📡 {self.agent_id}: Consuming alerts for patient {case_id}")
            
            while self.running:
                try:
                    message_batch = consumer.poll(timeout_ms=1000)
                    
                    if message_batch:
                        for topic_partition, messages in message_batch.items():
                            for message in messages:
                                if not self.running:
                                    return
                                
                                alert_data = {
                                    'case_id': case_id,
                                    'alert_content': message.value,
                                    'timestamp': time.time()
                                }
                                
                                print(f"🚨 {self.agent_id}: Alert received for patient {case_id}")
                                
                                # Queue for processing
                                try:
                                    self.processing_queue.put(alert_data, block=False)
                                except queue.Full:
                                    print(f"⚠️ {self.agent_id}: Queue full! Dropping alert for patient {case_id}")
                
                except Exception as e:
                    print(f"❌ {self.agent_id}: Polling error for patient {case_id}: {e}")
                    if not self.running:
                        break
                    time.sleep(2)
        
        except Exception as e:
            print(f"❌ {self.agent_id}: Consumer setup error for patient {case_id}: {e}")
        finally:
            try:
                consumer.close()
            except:
                pass

    def worker_loop(self):
        """Worker thread processes all alerts for this agent"""
        print(f"🔧 {self.agent_id}: Worker started")
        
        while self.running:
            try:
                alert = self.processing_queue.get(timeout=1.0)
                
                if alert is None:  # Poison pill
                    break
                
                print(f"🔧 {self.agent_id}: Processing alert for patient {alert['case_id']}")
                self.process_alert(alert)
                
                self.processing_queue.task_done()
                
            except queue.Empty:
                continue
            except Exception as e:
                print(f"❌ {self.agent_id}: Worker error: {e}")
        
        print(f"🔧 {self.agent_id}: Worker stopped")

    def process_alert(self, alert_data):
        """Process alert and make decisions"""
        case_id = alert_data['case_id']
        alert_content = alert_data['alert_content']
        
        try:
            # Get available doctors from Redis
            available_doctors = list(redis_client.smembers("available_doctors"))
            
            if not available_doctors:
                print(f"⚠️ {self.agent_id}: No doctors available for patient {case_id}")
                return
            
            # Make decision using Gemini
            decision = self.make_gemini_decision(case_id, alert_content, available_doctors)
            
            if decision:
                assigned_doctor = decision.get('assigned_doctor')
                
                # Try to assign doctor (atomic operation)
                if self.assign_doctor_atomically(assigned_doctor, case_id):
                    # Success - create final decision
                    final_decision = {
                        'agent_id': self.agent_id,
                        'case_id': case_id,
                        'timestamp': alert_data['timestamp'],
                        'original_alert': alert_content,
                        'priority': decision.get('priority'),
                        'assigned_doctor': assigned_doctor,
                        'reasoning': decision.get('reasoning'),
                        'processing_time': time.time() - alert_data['timestamp']
                    }
                    
                    # Publish decision
                    producer.send(
                        topic=f"medical_decisions_patient_{case_id}",
                        value=final_decision
                    )
                    room = f"gemini_alert_patient_{case_id}"
                    data = {
                        'payload': {
                            'alert':final_decision
                        },
                        'room': room
                    }
                    
                    try:
                        if self.sio and self.sio.connected:
                            self.sio.emit('room_data', data)
                    except Exception as e:
                        print(f"Socket send failed: {e}")
                    
                    # Send urgent alerts to priority queue
                    if decision.get('priority') == 'urgent':
                        producer.send(
                            topic="urgent_medical_alerts",
                            value=final_decision
                        )
                    
                    print(f"✅ {self.agent_id}: Patient {case_id} - Priority: {decision.get('priority')}, Doctor: {assigned_doctor}")
                
                else:
                    print(f"⚠️ {self.agent_id}: Doctor {assigned_doctor} not available for patient {case_id}, retrying...")
                    # Could retry with different doctor or use fallback
            
        except Exception as e:
            print(f"❌ {self.agent_id}: Error processing patient {case_id}: {e}")

    def assign_doctor_atomically(self, doctor_name, case_id):
        """Atomically assign doctor using Redis transaction"""
        try:
            pipe = redis_client.pipeline()
            
            # Watch the available doctors set
            pipe.watch("available_doctors")
            
            # Check if doctor is still available
            if redis_client.sismember("available_doctors", doctor_name):
                # Start transaction
                pipe.multi()
                
                # Remove from available
                pipe.srem("available_doctors", doctor_name)
                
                # Add to assigned with patient info
                pipe.hset("assigned_doctors", doctor_name, json.dumps({
                    "patient_id": case_id,
                    "assigned_at": time.time(),
                    "assigned_by": self.agent_id
                }))
                
                # Execute transaction
                pipe.execute()
                
                print(f"🏥 {self.agent_id}: Doctor {doctor_name} assigned to patient {case_id}")
                return True
            else:
                print(f"⚠️ {self.agent_id}: Doctor {doctor_name} no longer available")
                return False
                
        except redis.WatchError:
            print(f"⚠️ {self.agent_id}: Concurrent modification detected, doctor assignment failed")
            return False
        except Exception as e:
            print(f"❌ {self.agent_id}: Redis error during doctor assignment: {e}")
            return False

    def make_gemini_decision(self, case_id, alert_content, available_doctors, max_retries=2):
        """Make decision using Gemini"""
        if not self.llm:
            return self.fallback_decision(case_id, alert_content, available_doctors)
        
        print(f"🧠 {self.agent_id}: Making Gemini decision for patient {case_id}")
        
        for attempt in range(max_retries):
            try:
                # Format prompt
                formatted_prompt = self.decision_prompt.format_messages(
                    case_id=case_id,
                    alert_content=alert_content,
                    available_doctors=available_doctors
                )
                
                # Get Gemini response
                response = self.llm.invoke(formatted_prompt)
                
                # Parse JSON response
                try:
                    decision = json.loads(response.content.strip())
                    
                    # Validate response
                    if (decision.get('priority') in ['urgent', 'high', 'moderate', 'negligible'] and
                        decision.get('assigned_doctor') in available_doctors):
                        
                        print(f"✅ {self.agent_id}: Gemini decision successful for patient {case_id}")
                        return decision
                    else:
                        print(f"⚠️ {self.agent_id}: Invalid Gemini response format, using fallback")
                        return self.fallback_decision(case_id, alert_content, available_doctors)
                
                except json.JSONDecodeError:
                    # Try to extract JSON from response
                    import re
                    json_match = re.search(r'\{.*\}', response.content, re.DOTALL)
                    if json_match:
                        decision = json.loads(json_match.group())
                        return decision
                    else:
                        raise ValueError("No valid JSON in response")
            
            except Exception as e:
                print(f"❌ {self.agent_id}: Gemini attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(1)
        
        print(f"❌ {self.agent_id}: All Gemini attempts failed, using fallback")
        return self.fallback_decision(case_id, alert_content, available_doctors)

    def fallback_decision(self, case_id, alert_content, available_doctors):
        """Simple fallback when Gemini fails"""
        alert_lower = str(alert_content).lower()
        
        # Simple priority logic
        if any(word in alert_lower for word in ['flatline', 'critical', 'urgent', 'emergency']):
            priority = 'urgent'
            # Prefer emergency/critical care doctors
            preferred_doctors = [d for d in available_doctors if 'emergency' in d or 'critical' in d]
        elif any(word in alert_lower for word in ['high', 'warning', 'abnormal']):
            priority = 'high'
            preferred_doctors = [d for d in available_doctors if 'icu' in d or 'emergency' in d]
        elif any(word in alert_lower for word in ['moderate', 'concerning']):
            priority = 'moderate'
            preferred_doctors = available_doctors
        else:
            priority = 'negligible'
            preferred_doctors = [d for d in available_doctors if 'general' in d]
        
        # Choose first available doctor from preferred list, or any available
        assigned_doctor = (preferred_doctors[0] if preferred_doctors 
                          else available_doctors[0] if available_doctors 
                          else "no_doctor_available")
        
        return {
            'priority': priority,
            'assigned_doctor': assigned_doctor,
            'reasoning': f'Fallback rule-based decision for patient {case_id}'
        }

    def on_stop(self):
        print(f"🛑 {self.agent_id}: Stopping...")
        self.running = False
        
        # Send poison pill to worker
        try:
            self.processing_queue.put(None, timeout=1)
        except queue.Full:
            pass
        
        # Wait for threads
        if self.worker_thread:
            self.worker_thread.join(timeout=3)
        
        for thread in self.consumer_threads:
            thread.join(timeout=1)
        
        print(f"✅ {self.agent_id}: Stopped")

    def on_receive(self, message):
        """Handle direct messages"""
        try:
            self.processing_queue.put(message, block=False)
        except queue.Full:
            print(f"⚠️ {self.agent_id}: Queue full, dropping message")


class GeminiSupervisor(pykka.ThreadingActor):
    def __init__(self):
        super().__init__()
        self.agents = {}

    def on_start(self):
        # Split patients between two agents
        mid_point = len(caseids_list) // 2
        
        patient_list_1 = caseids_list[:mid_point]  # First 15 patients
        patient_list_2 = caseids_list[mid_point:]  # Remaining patients
        
        print(f"📊 Agent 1 will handle patients: {patient_list_1}")
        print(f"📊 Agent 2 will handle patients: {patient_list_2}")
        
        # Start two Gemini agents
        self.agents['agent_1'] = GeminiAgent.start(
            agent_id="GeminiAgent_1",
            api_key=GEMINI_API,
            patient_list=patient_list_1
        )
        
        self.agents['agent_2'] = GeminiAgent.start(
            agent_id="GeminiAgent_2", 
            api_key=GEMINI_API_2,
            patient_list=patient_list_2
        )
        
        print("🧠 GeminiSupervisor: Both agents started successfully")

    def on_stop(self):
        print("🛑 GeminiSupervisor: Stopping all agents...")
        
        for agent_name, agent_ref in self.agents.items():
            print(f"🛑 Stopping {agent_name}...")
            agent_ref.stop()
        
        print("✅ GeminiSupervisor: All agents stopped")

# Usage
if __name__ == "__main__":
    print("🧠 Starting Gemini Decision System...")
    
    try:
        # Test Redis connection
        redis_client.ping()
        print("✅ Redis connection successful")
        
        supervisor = GeminiSupervisor.start()
        
        print("✅ Gemini system started. Monitoring for alerts...")
        print("🏥 Available doctors initialized in Redis")
        print("📡 Two agents consuming alerts from Groq...")
        
        # Keep running
        while True:
            time.sleep(5)
            
            # Optional: Print Redis status
            try:
                available_count = redis_client.scard("available_doctors")
                assigned_count = redis_client.hlen("assigned_doctors")
                print(f"🏥 Doctors - Available: {available_count}, Assigned: {assigned_count}")
            except:
                pass
            
    except KeyboardInterrupt:
        print("\n🛑 Shutting down Gemini system...")
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        if 'supervisor' in locals():
            supervisor.stop()
        
        time.sleep(2)
        print("✅ Gemini system stopped successfully")
        
        import pykka
        pykka.ActorRegistry.stop_all()