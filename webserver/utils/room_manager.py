import re
from typing import Optional, Tuple, List
from enum import Enum

class RoomType(Enum):
    PATIENT = "patient"
    SIGNAL = "signal"
    INVALID = "invalid"

class RoomManager:
    # Room naming patterns
    PATIENT_ROOM_PATTERN = r'^patient_(\d+)$'
    SIGNAL_ROOM_PATTERN = r'^patient_(\d+)_([A-Za-z0-9]+)$'
    
    # Valid signal types for your ICU system
    VALID_SIGNALS = {'ECG', 'SpO2', 'ABP', 'RESP', 'TEMP', 'HR', 'BP'}
    
    @classmethod
    def parse_room(cls, room_name: str) -> Tuple[RoomType, Optional[str], Optional[str]]:
        """
        Parse room name and return (type, case_id, signal_type)
        
        Examples:
        - 'patient_123' → (PATIENT, '123', None)
        - 'patient_123_ECG' → (SIGNAL, '123', 'ECG')
        - 'invalid_room' → (INVALID, None, None)
        """
        if not room_name:
            return RoomType.INVALID, None, None
            
        # Try signal room first (more specific)
        signal_match = re.match(cls.SIGNAL_ROOM_PATTERN, room_name)
        if signal_match:
            case_id = signal_match.group(1)
            signal_type = signal_match.group(2).upper()
            
            # Validate signal type
            if signal_type in cls.VALID_SIGNALS:
                return RoomType.SIGNAL, case_id, signal_type
            else:
                return RoomType.INVALID, None, None
        
        # Try patient room
        patient_match = re.match(cls.PATIENT_ROOM_PATTERN, room_name)
        if patient_match:
            case_id = patient_match.group(1)
            return RoomType.PATIENT, case_id, None
            
        return RoomType.INVALID, None, None
    
    @classmethod
    def create_patient_room(cls, case_id: str) -> str:
        """Create patient room name"""
        return f"patient_{case_id}"
    
    @classmethod
    def create_signal_room(cls, case_id: str, signal_type: str) -> str:
        """Create signal room name"""
        return f"patient_{case_id}_{signal_type.upper()}"
    
    @classmethod
    def get_parent_room(cls, room_name: str) -> Optional[str]:
        """Get parent patient room for a signal room"""
        room_type, case_id, signal_type = cls.parse_room(room_name)
        if room_type == RoomType.SIGNAL and case_id:
            return cls.create_patient_room(case_id)
        return None
    
    @classmethod
    def validate_room(cls, room_name: str) -> bool:
        """Check if room name is valid"""
        room_type, _, _ = cls.parse_room(room_name)
        return room_type != RoomType.INVALID