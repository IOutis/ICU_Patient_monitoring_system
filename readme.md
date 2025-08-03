# ICU Patient Monitoring System

An actor-based, real-time patient monitoring system for the ICU, designed to simulate a hospital environment. It uses Apache Kafka for streaming patient vital signals and leverages LLMs (Gemini and Groq) for intelligent alert analysis, all presented on a Flask-based real-time dashboard.

## ✅ Project Overview

* A real-time ICU patient monitoring system.
* Streams ECG, SpO₂, ABP signals etc.
* Uses Pykka actors + Kafka for scalable processing.
* Uses LLMs (Gemini + Groq) for intelligent alert generation.
* Frontend built with Flask + Socket.IO.
* Simulates real-world hospital data environment.

## 🧠 Core Features

* Actor-based architecture (Supervisor > PatientActor > SignalActor).
* Kafka-driven streaming of patient vitals with 500ms delay.
* LLM actors (Gemini + Groq) for analyzing medical alerts.
* WebSocket-based real-time frontend with signal charts + alerts.
* Rule-based anomaly detection (e.g., flatline, threshold breaches).
* Multi-patient management and signal isolation per patient.

## 🛠️ Setup Instructions

### 1. Clone & Setup Environment

```bash
git clone https://github.com/IOutis/ICU_Patient_monitoring_system.git
cd ICU_Patient_monitoring_system
python -m venv monitoring_venv
source monitoring_venv/bin/activate  # On Windows: monitoring_venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Ensure Kafka Is Running

* Kafka must be running on default ports (`localhost:9092`).
* Required topics must be created beforehand.
* You can use Docker or a local installation to run Kafka.

## 🚀 How to Run the System (in 5 Terminals)

Run the following **in separate terminals** from the root directory:

```bash
# 1. Gemini LLM Actor
python -m monitoring_system.gemini_llm_class

# 2. Groq LLM Actor
python -m monitoring_system.groq_llm_class

# 3. Flask + Socket.IO Webserver
python -m webserver.server

# 4. Kafka Producer (streams vitals every 500ms)
python monitoring_system/pykka_producer.py

# 5. Kafka Consumer + Actor System
python -m monitoring_system.pykka_consumer
```

These processes are designed to run concurrently and communicate via Kafka + actor messaging.

## 🌐 Access the Dashboard

Open in your browser:

```
http://localhost:5000
```

You'll see:
* Live patient signal graphs (ECG, SpO₂, ABP etc.)
* Real-time anomaly alerts
* Streamed via WebSocket + actor signals

## 🎥 Demo Video

[Demo video](https://drive.google.com/file/d/1xmH_dKmBpkqWQKvAhuTnRLLy003aiCNU/view?usp=sharing)

## 🧠 Technologies Used

* **Python**: Main backend language
* **Pykka**: Actor model framework
* **Kafka**: Message queue for real-time signal streaming
* **Flask + Socket.IO**: Real-time web frontend
* **LLMs**: Gemini + Groq for intelligent alert handling
* **HTML/JS**: Dashboard frontend templates

## ⚙️ Customization & Environment

* A `.env` file can store:
  * `GEMINI_API`
  * `GEMINI_API_2`
  * `GROQ_API_1`
  * `GROQ_API_2`
  * `GROQ_API_3`
  * Kafka topic names
* You can modify signal file paths or thresholds in `pykka_producer.py` and the consumer logic.

## ✅ Future Plans

* Add model-based anomaly detection (if labeled data is available)
* Add patient authentication/login
* Switch to a WebSocket backend with FastAPI for scalability
* Dockerize the full pipeline for easier deployment

## 🧑 Author

**Mohd Mushtaq**  
Aspiring AI Agent Engineer | Full-Stack Developer  
GitHub: https://github.com/IOutis