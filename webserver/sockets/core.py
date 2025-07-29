from flask_socketio import join_room, leave_room, disconnect, emit
from flask import request
from ..socketio_instance import socketio
from ..utils.room_manager import RoomManager, RoomType
import logging

logger = logging.getLogger(__name__)

# Track client rooms for cleanup
client_rooms = {}

@socketio.on('connect')
def on_connect():
    client_id = request.sid
    client_rooms[client_id] = set()
    logger.info(f"Client {client_id} connected")
    
    # Send connection acknowledgment
    emit('connection_ack', {
        'status': 'connected',
        'client_id': client_id,
        'server_time': '2025-07-22T08:45:00'  # Use actual timestamp
    })

@socketio.on('disconnect')
def on_disconnect():
    client_id = request.sid
    
    # Clean up client room tracking
    if client_id in client_rooms:
        rooms = client_rooms[client_id].copy()
        for room in rooms:
            leave_room(room)
            logger.info(f"Client {client_id} auto-left room: {room}")
        del client_rooms[client_id]
    
    logger.info(f"Client {client_id} disconnected")

@socketio.on('join_room')
def handle_join_room(data):
    """
    Handle room join requests
    Expected data: {'room': 'patient_123' or 'patient_123_ECG'}
    """
    client_id = request.sid
    room_name = data.get('room', '').strip()
    
    if not room_name:
        emit('error', {'message': 'Room name is required'})
        return
    
    # Validate room name
    if not RoomManager.validate_room(room_name):
        emit('error', {'message': f'Invalid room name: {room_name}'})
        return
    
    # Parse room details
    room_type, case_id, signal_type = RoomManager.parse_room(room_name)
    
    # Join the room
    join_room(room_name)
    client_rooms.setdefault(client_id, set()).add(room_name)
    
    # Log and respond
    logger.info(f"Client {client_id} joined room: {room_name} (case_id={case_id}, signal={signal_type})")
    
    emit('room_joined', {
        'room': room_name,
        'room_type': room_type.value,
        'case_id': case_id,
        'signal_type': signal_type,
        'timestamp': '2025-07-22T08:45:00'  # Use actual timestamp
    })
    
    # Auto-join parent room for signal rooms (optional)
    if room_type == RoomType.SIGNAL:
        parent_room = RoomManager.get_parent_room(room_name)
        if parent_room and parent_room not in client_rooms[client_id]:
            join_room(parent_room)
            client_rooms[client_id].add(parent_room)
            logger.info(f"Client {client_id} auto-joined parent room: {parent_room}")

@socketio.on('leave_room')
def handle_leave_room(data):
    """Handle room leave requests"""
    client_id = request.sid
    room_name = data.get('room', '').strip()
    
    if not room_name:
        emit('error', {'message': 'Room name is required'})
        return
    
    if room_name in client_rooms.get(client_id, set()):
        leave_room(room_name)
        client_rooms[client_id].discard(room_name)
        logger.info(f"Client {client_id} left room: {room_name}")
        
        emit('room_left', {
            'room': room_name,
            'timestamp': '2025-07-22T08:45:00'
        })
    else:
        emit('error', {'message': f'You are not in room: {room_name}'})

@socketio.on('list_rooms')
def handle_list_rooms():
    """List rooms client is currently in"""
    client_id = request.sid
    import sys
    rooms = list(client_rooms.get(client_id, set()))
    logger.info("ROOMS = ",rooms)
    sys.stdout.flush()

    emit('rooms_list', {
        'rooms': rooms,
        'count': len(rooms)
    })

@socketio.on_error_default
def default_error_handler(e):
    """Global error handler"""
    client_id = request.sid
    logger.error(f"Socket error from client {client_id}: {str(e)}")
    emit('error', {'message': 'An unexpected error occurred'})