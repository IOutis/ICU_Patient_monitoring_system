import os
print(os.getcwd())
from webserver.app import create_app
from webserver.socketio_instance import socketio
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

app = create_app()

if __name__ == '__main__':
    print("🏥 Starting ICU Patient Monitoring WebServer...")
    print("📡 Socket.IO endpoints available:")
    print("   - connect/disconnect")
    print("   - join_room/leave_room")
    print("   - Patient rooms: patient_{case_id}")
    print("   - Signal rooms: patient_{case_id}_{signal}")
    print("   - Valid signals: ECG, SpO2, ABP, RESP, TEMP, HR, BP")
    
    socketio.run(
        app, 
        host='0.0.0.0', 
        port=5000,
        debug=False  # Set to True for development
    )