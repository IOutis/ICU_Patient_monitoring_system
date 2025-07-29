from flask_socketio import SocketIO, join_room, emit
from flask import Flask, request

app = Flask(__name__)
app.config['SECRET_KEY'] = 'secret!'
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')

@socketio.on('join')
def on_join(data):
    room = data.get('room')
    join_room(room)
    print(f"Client {request.sid} joined room: {room}")
    emit('joined', {'room': room})
