from langchain.chat_models import init_chat_model
from langchain_groq import ChatGroq
import os
import pykka
from typing import Union
from utils.llm_tools import *
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain_core.messages import HumanMessage
from langchain.agents import AgentExecutor, create_tool_calling_agent
from collections import deque
from dotenv import load_dotenv
import json
import threading
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from concurrent.futures import ThreadPoolExecutor
from kafka import KafkaProducer
import queue
load_dotenv()
producer = KafkaProducer(
            bootstrap_servers='localhost:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
GROQ_API_1 = os.getenv("GROQ_API_1")
GROQ_API_2 = os.getenv("GROQ_API_2")
GROQ_API_3 = os.getenv("GROQ_API_3")
GEMINI_API = os.getenv("GEMINI_API")
import time
# Set up logging for retry attempts

from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaConsumer
from kafka.errors import TopicAlreadyExistsError
import json

def create_kafka_consumer(topic: str):
    # Ensure the topic exists using KafkaAdminClient
    try:
        admin_client = KafkaAdminClient(bootstrap_servers="localhost:9092")
        existing_topics = admin_client.list_topics()

        if topic not in existing_topics:
            new_topic = NewTopic(name=topic, num_partitions=1, replication_factor=1)
            admin_client.create_topics([new_topic])
            print(f"[Kafka] Created topic: {topic}")
        else:
            print(f"[Kafka] Topic '{topic}' already exists")
        
        admin_client.close()
    except TopicAlreadyExistsError:
        print(f"[Kafka] Topic already exists (caught)")
    except Exception as e:
        print(f"[Kafka] Admin error: {e}")

    # Create and return the KafkaConsumer
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers="localhost:9092",
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    print(f"[Kafka] Consumer created for topic: {topic}")
    return consumer


def process_with_agent(agent_executor:AgentExecutor, message, max_retries=3):
    """Non-blocking retry with shorter waits"""
    for attempt in range(max_retries):
        # try:
            return agent_executor.invoke(message)
        # except Exception as e:
        #     if attempt < max_retries - 1:
        #         wait_time = min(2 ** attempt, 4)  # 1, 2, 4 seconds max
        #         print(f"Retrying in {wait_time}s... (attempt {attempt + 1}), {e}")
        #         time.sleep(wait_time)
        #     else:
        #         print(f"Failed after {max_retries} attempts: {e}")
        #         raise


class LLMActor(pykka.ThreadingActor):
    def __init__(self, api_key: str, model_name: str, model_provider: str,parent_ref,case_id):
        """model_name : gemma2-9b-it --> model_provider : groq
           model_name : gemini-2.0-flash --> model_provider : google_genai"""
        super().__init__()
        self.api_key = api_key
        self.caseid = case_id
        self.parent_ref = parent_ref
        self.model_name = model_name
        self.model_provider = model_provider
        self.tools = [calculate_shock_index, calculate_mean_arterial_pressure, 
                     check_cushings_triad, check_sepsis_warning]
        # self.producer = producer
        self.processing_queue = queue.Queue(maxsize=100)  # Buffer for alerts
        self.worker_thread = None
        # Initialize LLM with better configuration
        self.llm = init_chat_model(
            self.model_name, 
            model_provider=self.model_provider, 
            api_key=self.api_key,
            temperature=0.1  # Lower temperature for more consistent tool calling
        )
        
        self.tool_results_memory = deque(maxlen=20)
        # Improved prompt with clearer instructions
        self.prompt = ChatPromptTemplate.from_messages([
            ("system", """You are a medical assistant analyzing patient monitoring data. 

CRITICAL: You can ONLY use these exact tools that are available:
1. calculate_shock_index - needs HR and SBP data  
2. calculate_mean_arterial_pressure - needs SBP and DBP data
3. check_sepsis_warning - needs BT and HR data
4. check_cushings_triad - needs SBP, HR, and respiration data

DO NOT attempt to call any other tools like 'extract_vital_signs' or any tool not in the above list.

INSTRUCTIONS:
1. You will receive a list of medical device signal events
2. Each event has: signal_value, time_recorded, caseid, category, reason
3. First analyze what vital signs are available in the data
4. Only use tools for which you have the required data
5. If you don't have required data for a tool, skip it and explain why
6. Provide severity assessment: negligible, moderate, high, or urgent
7. Focus on the specific alerts in the data (flatline, z_score alerts)

Example: If you only see HR and SpO2 data, don't try to calculate shock index (needs SBP) or MAP (needs SBP+DBP).
Instead, analyze the available data directly and note any concerning patterns."""),
            ("user", "{input}"),
            MessagesPlaceholder(variable_name="agent_scratchpad"),
        ])
        
        agent = create_tool_calling_agent(self.llm, self.tools, self.prompt)
        self.agent_executor = AgentExecutor(
            agent=agent, 
            tools=self.tools, 
            verbose=True,
            return_intermediate_steps=True,
            max_iterations=3,  # Reduce to prevent tool hallucination
            handle_parsing_errors=True,
            early_stopping_method="generate"
        )
        self.consumer = None
    
    def on_start(self):
        self.running = True  # ← MISSING
        # Start single worker thread for this patient
        self.worker_thread = threading.Thread(
            target=self._worker_loop, 
            name=f"Worker-Patient-{self.caseid}",
            daemon=True
        )
        self.worker_thread.start()
        
        # Start consumer thread
        self.consumer_thread = threading.Thread(
            target=self._consume_loop, 
            name=f"Consumer-Patient-{self.caseid}",
            daemon=True
        )
        self.consumer_thread.start()
        
    
    def _worker_loop(self):
        """Single worker thread processes all messages for this patient"""
        print(f"[Worker-{self.caseid}] Started processing loop")
        
        while self.running:
            try:
                # Get next message to process (blocking with timeout)
                message = self.processing_queue.get(timeout=1.0)
                
                if message is None:  # Poison pill to stop
                    break
                
                print(f"[Worker-{self.caseid}] Processing message...")
                self._handle_message(message)
                
                # Mark task as done
                self.processing_queue.task_done()
                
            except queue.Empty:
                # No messages, continue loop (will check self.running)
                continue
            except Exception as e:
                print(f"[Worker-{self.caseid}] Processing error: {e}")
        
        print(f"[Worker-{self.caseid}] Worker loop ended")
    
    def _consume_loop(self):
        """Consumer thread gets messages from Kafka and queues them"""
        print(f"[Consumer-{self.caseid}] Starting Kafka consumer...")
        topic = f"llm_alert_patient_{self.caseid}"
        
        try:
            self.consumer = create_kafka_consumer(topic)
        except Exception as e:
            print(f"[Consumer-{self.caseid}] Failed to create consumer: {e}")
            return
        
        while self.running:
            try:
                message_batch = self.consumer.poll(timeout_ms=1000)
                
                if message_batch:
                    for topic_partition, messages in message_batch.items():
                        for message in messages:
                            if not self.running:
                                return
                                
                            decoded_message = message.value
                            print(f"[Consumer-{self.caseid}] Received: {decoded_message}")
                            
                            # Queue for processing (non-blocking)
                            try:
                                self.processing_queue.put(decoded_message, block=False)
                                print(f"[Consumer-{self.caseid}] Queued for processing")
                            except queue.Full:
                                print(f"[Consumer-{self.caseid}] ⚠️  Processing queue full! Dropping message")
                                # In ICU, you might want to log this as critical
                
            except Exception as e:
                print(f"[Consumer-{self.caseid}] Kafka error: {e}")
                if not self.running:
                    break
                time.sleep(2)
        
        print(f"[Consumer-{self.caseid}] Consumer loop ended")

                    
    def on_stop(self):
        print(f"[Patient-{self.caseid}] Stopping...")
        self.running = False
        
        # Stop worker thread gracefully
        try:
            self.processing_queue.put(None, timeout=1)  # Poison pill
        except queue.Full:
            pass
        
        # Close Kafka consumer
        if self.consumer:
            self.consumer.close()
        
        # Wait for threads to finish
        if hasattr(self, 'consumer_thread'):
            print(f"[Patient-{self.caseid}] Waiting for consumer thread...")
            self.consumer_thread.join(timeout=3)
        
        if hasattr(self, 'worker_thread'):
            print(f"[Patient-{self.caseid}] Waiting for worker thread...")
            self.worker_thread.join(timeout=3)
        
        print(f"[Patient-{self.caseid}] Stopped ✅")


    def on_receive(self, message):
        """Handle direct messages by queuing them"""
        try:
            self.processing_queue.put(message, block=False)
        except queue.Full:
            print(f"[Actor-{self.caseid}] Processing queue full, dropping message")

    def _handle_message(self, message):
        try:
            if not message:
                print("No prompt provided")
                return
            print(f"[LLMActor] Received message: {message}")
            
            response = process_with_agent(self.agent_executor, message)

            print(f"[LLMActor]{self.caseid}: LLM Response -> {response}")
            producer.send(topic=f"llm_generated_patient_{self.caseid}")

            if "intermediate_steps" in response:
                for action, result in response["intermediate_steps"]:
                    self.tool_results_memory.append({
                        "tool_called": action.tool,
                        "tool_input": action.tool_input,
                        "tool_output": result,
                    })
        except Exception as e:
            print(f"[LLMActor] Error: {e}")

    def _fallback_analysis(self, message):
        """Fallback analysis when agent fails"""
        input_text = message.get('input', str(message))
        
        # Try to extract JSON data from the input
        import re
        json_match = re.search(r'\[.*\]', input_text, re.DOTALL)
        if json_match:
            try:
                events_data = json.loads(json_match.group())
                return self._analyze_events_directly(events_data)
            except:
                pass
        
        return "Could not process the medical data. Please check the input format."
    
    def _analyze_events_directly(self, events):
        """Direct analysis of events without LLM"""
        analysis = "FALLBACK MEDICAL ANALYSIS:\n\n"
        severity = "negligible"
        alerts = []
        
        # Analyze the events directly
        vital_signs = {}
        for event in events:
            category = event.get('category', '')
            value = event.get('signal_value', '')
            reason = event.get('reason', '')
            
            if category not in vital_signs:
                vital_signs[category] = []
            vital_signs[category].append({'value': value, 'reason': reason})
            
            # Check for critical alerts
            if 'flatline' in reason.lower():
                alerts.append(f"CRITICAL: {category} flatline detected (value: {value})")
                severity = "urgent"
            elif 'z_score' in reason.lower():
                alerts.append(f"WARNING: {category} abnormal reading - {reason}")
                if severity != "urgent":
                    severity = "high"
        
        analysis += f"Vital Signs Found: {list(vital_signs.keys())}\n"
        analysis += f"Total Events: {len(events)}\n\n"
        
        if alerts:
            analysis += "ALERTS:\n"
            for alert in alerts:
                analysis += f"• {alert}\n"
            analysis += "\n"
        
        analysis += f"OVERALL SEVERITY: {severity.upper()}\n"
        
        return analysis


# Alternative: Class-level retry decorator for specific methods
class RetryConfig:
    """Configuration for retry behavior"""
    MAX_ATTEMPTS = 3
    MIN_WAIT = 2
    MAX_WAIT = 8
    MULTIPLIER = 1

caseids_list = [609, 634, 1032, 1087, 1209, 1488, 1903, 1952, 2191, 2259, 2327, 2340, 2422, 2626, 2724, 2880, 3519, 3638, 3984, 4658, 4721, 4978, 5107, 5211, 5248, 5337, 5540, 5841, 6234, 6335]
class SupervisorLLMActor(pykka.ThreadingActor):
    def __init__(self):
        super().__init__()
        self.llm_actors = {}
        self.api_keys = [GROQ_API_1,GROQ_API_2,GROQ_API_3]
        self.count = 1
    def on_start(self):
        for caseid in caseids_list:
            llm_actor = LLMActor.start(self.api_keys[self.count%3],model_name="gemma2-9b-it",model_provider="groq",parent_ref=self.actor_ref,case_id=caseid)
            self.llm_actors[caseid] = llm_actor
            self.count+=1
        

if __name__ == "__main__":
    print("🚀 Starting SupervisorLLMActor...")
    supervisor_ref = SupervisorLLMActor.start()

    try:
        print("⏳ Waiting for all LLM actors to start...")
        # time.sleep(10)  # Let actors start and begin consuming

    except Exception as e:
        print(f"❌ Error: {e}")
        print("🛑 Stopping all actors...")
        supervisor_ref.stop()
        print("✅ All actors stopped successfully.")