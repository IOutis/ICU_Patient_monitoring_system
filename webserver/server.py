from webserver.socketio_instance import socketio, app
from flask import request
from flask_socketio import join_room

@socketio.on('connect')
def handle_connect():
    print(f"Client connected: {request.sid}")

@socketio.on('join')
def on_join(data):
    room = data.get('room')
    join_room(room)
    print(f"Client {request.sid} joined room {room}")
    socketio.emit('joined', {'room': room}, to=request.sid)
@socketio.on('room_data')
def forward_to_room(data):
    room = data['room']
    payload = data['payload']
    socketio.emit('room_data', payload, to=room)
    
@socketio.on('alert')
def forward_alert_to_room(data):
    room = data['room']
    payload = data['payload']
    print("Alert received",room)
    socketio.emit('alert', payload, to=room)
    
@socketio.on('disconnect')
def handle_disconnect():
    print(f"Client disconnected: {request.sid}")
# import threading
# import time

# def emit_test_data():
#     time.sleep(5)
#     test_data = {
#         'signal_name': 'HR',
#         'time_recorded': '2025-07-22 12:00:00',
#         'signal_value': 89,
#         'category': 'HR',
#         'caseid': '609'
#     }
#     socketio.emit('room_data', test_data, room='patient_609_HR')
#     print("Test data emitted")

# threading.Thread(target=emit_test_data).start()

if __name__ == '__main__':
    socketio.run(app, host='0.0.0.0', port=5000, debug=True)
