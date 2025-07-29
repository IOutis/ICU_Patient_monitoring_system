import socketio

sio = socketio.Client()

@sio.event
def connect():
    print("Connected")
    room = 'alert_patient_634'
    sio.emit('join', {'room': room})

@sio.on('joined')
def joined(data):
    print(f"Joined room: {data['room']}")

@sio.on('room_data')
def handle_data(data):
    print(f"Received data:  {data.get('caseid')} = {data.get('signal_value')} ---- {data.get('category')}")
@sio.on('alert')
def handle_data(data):
    print(f"Received data:  {data}")

sio.connect('http://localhost:5000')
sio.wait()
