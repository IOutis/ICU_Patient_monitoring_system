# Updated Flask server to serve HTML and handle Socket.IO
from flask import Flask, render_template_string, send_from_directory
from flask_socketio import SocketIO, emit, join_room
import os
from flask import Flask, render_template, request
app = Flask(__name__)
app.config['SECRET_KEY'] = 'your-secret-key'
socketio = SocketIO(app, cors_allowed_origins="*")

# Serve the main HTML file
@app.route('/')
def index():
    return render_template('icu_monitoring.html')

# Alternative: serve static files
@app.route('/static/<path:filename>')
def static_files(filename):
    return send_from_directory('static', filename)

@socketio.on('connect')
def handle_connect():
    print(f"Client connected: {request.sid}")
    emit('connection_status', {'status': 'connected'})

@socketio.on('join')
def on_join(data):
    room = data.get('room')
    join_room(room)
    print(f"Client {request.sid} joined room {room}")
    emit('joined', {'room': room})

@socketio.on('room_data')
def forward_to_room(data):
    room = data['room']
    payload = data['payload']
    print(f"Forwarding data to room: {room}")
    emit('room_data', payload, room=room)
    
@socketio.on('alert')
def forward_alert_to_room(data):
    room = data['room']
    payload = data['payload']
    print(f"Alert received for room: {room}")
    emit('alert', payload, room=room)
    
@socketio.on('disconnect')
def handle_disconnect():
    print(f"Client disconnected: {request.sid}")

if __name__ == '__main__':
    print("🚀 Starting ICU Monitoring Server...")
    print("📱 Access the dashboard at: http://localhost:5000")
    socketio.run(app, host='0.0.0.0', port=5000, debug=True)