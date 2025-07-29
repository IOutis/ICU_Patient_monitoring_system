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
load_dotenv()

GROQ_API_1 = os.getenv("GROQ_API_1")
GROQ_API_2 = os.getenv("GROQ_API_2")
GROQ_API_3 = os.getenv("GROQ_API_3")
GEMINI_API = os.getenv("GEMINI_API")




class LLMActor(pykka.ThreadingActor):
    def __init__(self,api_key:str,model_name:str,model_provider:str):
        """model_name : gemma2-9b-it --> model_provider : groq
           model_name : gemini-2.0-flash --> model_provider : google_genai"""
        super().__init__()
        self.api_key=api_key
        self.model_name = model_name
        self.model_provider=model_provider
        self.tools=[calculate_shock_index,calculate_mean_arterial_pressure,check_cushings_triad,check_sepsis_warning]
        self.llm = init_chat_model(self.model_name,model_provider=self.model_provider,api_key = self.api_key)
        self.llm_with_tools = self.llm.bind_tools(self.tools)
        self.tool_results_memory = deque(maxlen=20)
        self.prompt = ChatPromptTemplate.from_messages([
            ("system", "You are a medical assistant. You must use the provided tools to answer questions. In your response please also include the tool used and their results."),
            ("user", "{input}"),
            MessagesPlaceholder(variable_name="agent_scratchpad"),
        ])
        agent = create_tool_calling_agent(self.llm, self.tools, self.prompt)
        self.agent_executor = AgentExecutor(agent=agent, tools=self.tools, verbose=True,return_intermediate_steps=True)
    def on_receive(self, message):
        response = self.agent_executor.invoke(message)
        if "intermediate_steps" in response and response["intermediate_steps"]:
        # The 'intermediate_steps' is a list of (action, result) tuples
            for action, result in response["intermediate_steps"]:
                log_entry = {
                    "tool_called": action.tool,
                    "tool_input": action.tool_input,
                    "tool_output": result
                }
                print(f"STORING LOG: {log_entry}")
                self.tool_results_memory.append(log_entry)

        return response['output'],self.tool_results_memory
    
    
    

    

if __name__ == "__main__":
    test_prompt_text = """ [{'signal_value': 100.0, 'time_recorded': 26.2797, 'caseid': 2626, 'category': 'SpO2', 'reason': 'flatline_detected'}, {'signal_value': 72.0, 'time_recorded': 24.2807, 'caseid': 2626, 'category': 'HR', 'reason': 'z_score_mild:2.20'}, {'signal_value': 100.0, 'time_recorded': 28.2797, 'caseid': 2626, 'category': 'SpO2', 'reason': 'flatline_detected'}, {'signal_value': 75.0, 'time_recorded': 30.2797, 'caseid': 2626, 'category': 'HR', 'reason': 'z_score_mild:2.01'}, {'signal_value': 100.0, 'time_recorded': 30.2797, 'caseid': 2626, 'category': 'SpO2', 'reason': 'flatline_detected'}, {'signal_value': 100.0, 'time_recorded': 32.2797, 'caseid': 2626, 'category': 'SpO2', 'reason': 'flatline_detected'}, {'signal_value': 100.0, 'time_recorded': 34.2797, 'caseid': 2626, 'category': 'SpO2', 'reason': 'flatline_detected'}, {'signal_value': 100.0, 'time_recorded': 36.2787, 'caseid': 2626, 'category': 'SpO2', 'reason': 'flatline_detected'}, {'signal_value': 10.0, 'time_recorded': 498.215, 'caseid': 2626, 'category': 'RR', 'reason': 'z_score_mild:2.18'}, {'signal_value': 13.0, 'time_recorded': 505.063, 'caseid': 2626, 'category': 'RR', 'reason': 'z_score_mild:2.46'}]
This is the data we received from the devices. Analyse them and say what's the issue. and what is the severity level(negligible, moderate, high, urgent). Extract the necessary data from the provided data and then pass the arguments appropriately"""

    # Start the actor
    gemma_actor_ref = LLMActor.start(
        api_key=GROQ_API_1,
        model_name="gemma2-9b-it",
        model_provider="groq"
    )

    try:
        # --- THIS IS THE FIX ---
        # Create a dictionary with the key 'input' to match the prompt
        test_message = {"input": test_prompt_text}

        print("Sending test prompt to agent...")
        response = gemma_actor_ref.ask(test_message)
        
        print("\n--- Agent Finished ---")
        print(f"Final Answer: {response}")

    finally:
        gemma_actor_ref.stop()