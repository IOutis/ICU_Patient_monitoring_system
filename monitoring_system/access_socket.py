# accessory_socket.py
import socketio

sio = socketio.Client()

def connect():
    try:
        if not sio.connected:
            sio.connect('http://localhost:5000')
    except Exception as e:
        print(f"[Socket Error] Could not connect: {e}")

def get_sio():
    if not sio.connected:
        try:
            sio.connect('http://localhost:5000')
        except Exception as e:
            print(f"[ERROR] get_sio() failed: {e}")
            return None
    return sio

def socket_ready():
    return sio.connected
