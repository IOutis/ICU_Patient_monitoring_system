from langchain.chat_models import init_chat_model
from langchain.agents import create_tool_calling_agent

from langchain.tools import Tool
import os
import pykka
from typing import Union
from dotenv import load_dotenv
load_dotenv()
GROQ_API_1 = os.getenv("GROQ_API_1")
GROQ_API_2 = os.getenv("GROQ_API_2")
GROQ_API_3 = os.getenv("GROQ_API_3")
GEMINI_API = os.getenv("GEMINI_API")
llm = init_chat_model(configurable_fields="any")
class LLMActor(pykka.ThreadingActor):
    def __init__(self,api_key:str,model_name:str,model_provider:str):
        super().__init__()
        self.api_key=api_key
        self.model_name = model_name
        self.model_provider=model_provider
        self.tools=[]
    def on_receive(self, message):
        response = llm.invoke(message,config={"configurable": {"model": self.model_name,'model_provider':self.model_provider,"api_key":self.api_key}})['content']
        return response



    