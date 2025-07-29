from typing import List, Dict, Optional
from langchain_core.tools import tool

# @tool
def calculate_shock_index(events: List[Dict]) -> dict:
    """
    Calculates the Shock Index from a list of recent signal events.

    This tool parses a list of event dictionaries to find the most recent
    heart rate (HR) and systolic blood pressure (SBP) to calculate the index.
    A value > 0.9 is generally considered critical.

    Args:
        events: A list of dictionaries, where each is a signal alert.
                Example event: {'category': 'HR', 'signal_value': 140}

    Returns:
        A dictionary containing the calculated value and a clinical assessment.
    """
    latest_hr = None
    latest_sbp = None

    # Parse the list in reverse to find the most recent HR and SBP
    for event in reversed(events):
        print(event)
        if event.get('category') == 'HR' and latest_hr is None:
            latest_hr = event.get('signal_value')
        if event.get('category') == 'SBP' and latest_sbp is None:
            latest_sbp = event.get('signal_value')
        if latest_hr and latest_sbp:
            break

    if latest_hr is None or latest_sbp is None:
        return {"error": "Insufficient HR or SBP data in event list."}

    if latest_sbp == 0:
        return {"value": None, "assessment": "Invalid SBP"}
    
    si = latest_hr / latest_sbp
    assessment = "Normal"
    if si > 0.9:
        assessment = "Critical"
    elif si > 0.7:
        assessment = "Abnormal"
    
    return {"value": round(si, 2), "assessment": assessment}

@tool
def calculate_mean_arterial_pressure(events: List[Dict]) -> dict:
    """
    Calculates Mean Arterial Pressure (MAP) from a list of signal events.

    This tool parses a list of events to find the most recent systolic (SBP)
    and diastolic (DBP) blood pressure values. A MAP less than 65 mmHg
    is concerning for organ perfusion.

    Args:
        events: A list of dictionaries, where each is a signal alert.
                Example event: {'category': 'SBP', 'signal_value': 120}

    Returns:
        A dictionary containing the calculated MAP and a clinical assessment.
    """
    latest_sbp = None
    latest_dbp = None

    for event in reversed(events):
        if event.get('category') == 'SBP' and latest_sbp is None:
            latest_sbp = event.get('signal_value')
        if event.get('category') == 'DBP' and latest_dbp is None:
            latest_dbp = event.get('signal_value')
        if latest_sbp and latest_dbp:
            break
            
    if latest_sbp is None or latest_dbp is None:
        return {"error": "Insufficient SBP or DBP data in event list."}

    map_value = (latest_sbp + 2 * latest_dbp) / 3
    assessment = "Normal"
    if map_value < 65:
        assessment = "Low Perfusion Warning"
    
    return {"value": round(map_value, 1), "assessment": assessment}

@tool
def check_sepsis_warning(events: List[Dict]) -> Optional[dict]:
    """
    Checks for a common early warning sign of sepsis from a list of events.

    This tool checks for the combination of fever (hyperthermia) and an
    abnormally high heart rate (tachycardia).

    Args:
        events: A list of dictionaries, where each is a signal alert.
                Example event: {'category': 'BT', 'signal_value': 39.0}

    Returns:
        A dictionary with an alert if the condition is met, otherwise None.
    """
    latest_temp = None
    latest_hr = None

    for event in reversed(events):
        # Note: Using 'BT' for Body Temperature as in your data example
        if event.get('category') == 'BT' and latest_temp is None:
            latest_temp = event.get('signal_value')
        if event.get('category') == 'HR' and latest_hr is None:
            latest_hr = event.get('signal_value')
        if latest_temp and latest_hr:
            break
            
    if latest_temp is None or latest_hr is None:
        return {"error": "Insufficient Temperature or HR data in event list."}

    is_fever = latest_temp > 38.5
    is_tachycardic = latest_hr > 120

    if is_fever and is_tachycardic:
        return {
            "alert": "Sepsis Early Warning: High fever with disproportionate tachycardia detected.",
            "is_critical": True
        }
    return None

@tool
def check_cushings_triad(events: List[Dict]) -> Optional[dict]:
    """
    Checks for Cushing's Triad from a list of recent signal events.

    This tool parses events to find recent SBP and HR. It assumes irregular
    respiration is flagged as a specific event type.

    Args:
        events: A list of dictionaries, where each is a signal alert.

    Returns:
        A dictionary with an alert if the triad is present, otherwise None.
    """
    latest_sbp = None
    latest_hr = None
    is_respiration_irregular = False

    for event in reversed(events):
        if event.get('category') == 'SBP' and latest_sbp is None:
            latest_sbp = event.get('signal_value')
        if event.get('category') == 'HR' and latest_hr is None:
            latest_hr = event.get('signal_value')
        # In a real system, you'd look for a specific respiration event
        if event.get('category') == 'RESP' and event.get('status') == 'irregular':
            is_respiration_irregular = True
        if latest_sbp and latest_hr:
            break

    if latest_sbp is None or latest_hr is None:
        return {"error": "Insufficient SBP or HR data in event list."}

    is_hypertensive = latest_sbp > 180
    is_bradycardic = latest_hr < 60

    if is_hypertensive and is_bradycardic and is_respiration_irregular:
        return {
            "alert": "Cushing's Triad Detected: Sign of critical intracranial pressure.",
            "is_critical": True
        }
    return None


print(calculate_shock_index( [{'signal_value': 100.0, 'time_recorded': 26.2797, 'caseid': 2626, 'category': 'SpO2', 'reason': 'flatline_detected'}, {'signal_value': 72.0, 'time_recorded': 24.2807, 'caseid': 2626, 'category': 'HR', 'reason': 'z_score_mild:2.20'}, {'signal_value': 100.0, 'time_recorded': 28.2797, 'caseid': 2626, 'category': 'SpO2', 'reason': 'flatline_detected'}, {'signal_value': 75.0, 'time_recorded': 30.2797, 'caseid': 2626, 'category': 'HR', 'reason': 'z_score_mild:2.01'}, {'signal_value': 100.0, 'time_recorded': 30.2797, 'caseid': 2626, 'category': 'SpO2', 'reason': 'flatline_detected'}, {'signal_value': 100.0, 'time_recorded': 32.2797, 'caseid': 2626, 'category': 'SpO2', 'reason': 'flatline_detected'}, {'signal_value': 100.0, 'time_recorded': 34.2797, 'caseid': 2626, 'category': 'SpO2', 'reason': 'flatline_detected'}, {'signal_value': 100.0, 'time_recorded': 36.2787, 'caseid': 2626, 'category': 'SpO2', 'reason': 'flatline_detected'}, {'signal_value': 10.0, 'time_recorded': 498.215, 'caseid': 2626, 'category': 'RR', 'reason': 'z_score_mild:2.18'}, {'signal_value': 13.0, 'time_recorded': 505.063, 'caseid': 2626, 'category': 'RR', 'reason': 'z_score_mild:2.46'}]))