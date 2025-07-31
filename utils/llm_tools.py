from typing import List, Dict, Optional, Union
from langchain_core.tools import tool
import json

@tool
def calculate_shock_index(events: Union[List[Dict], str]) -> dict:
    """
    Calculates the Shock Index from recent signal events.
    Shock Index = Heart Rate / Systolic Blood Pressure
    Normal: <0.7, Abnormal: 0.7-0.9, Critical: >0.9

    Args:
        events: A list of dictionaries or JSON string representing signal alerts 
                (e.g., [{'category': 'HR', 'signal_value': 140}, {'category': 'SBP', 'signal_value': 120}])

    Returns:
        A dictionary containing the calculated shock index and clinical assessment.
    """
    try:
        # Handle string input (JSON)
        if isinstance(events, str):
            events = json.loads(events)
        
        if not isinstance(events, list):
            return {"error": "Events must be a list of dictionaries"}
            
        latest_hr = None
        latest_sbp = None

        # Parse the list in reverse to find the most recent HR and SBP
        for event in reversed(events):
            if not isinstance(event, dict):
                continue
                
            category = event.get('category', '').upper()
            signal_value = event.get('signal_value')
            
            if category == 'HR' and latest_hr is None and signal_value is not None:
                latest_hr = float(signal_value)
            elif category == 'SBP' and latest_sbp is None and signal_value is not None:
                latest_sbp = float(signal_value)
                
            if latest_hr is not None and latest_sbp is not None:
                break

        if latest_hr is None:
            return {"error": "No Heart Rate (HR) data found in events"}
        if latest_sbp is None:
            return {"error": "No Systolic Blood Pressure (SBP) data found in events"}

        if latest_sbp == 0:
            return {"error": "Invalid SBP value (cannot be zero)"}
        
        si = latest_hr / latest_sbp
        
        if si > 0.9:
            assessment = "Critical"
            severity = "urgent"
        elif si > 0.7:
            assessment = "Abnormal"  
            severity = "high"
        else:
            assessment = "Normal"
            severity = "negligible"
        
        return {
            "shock_index": round(si, 2),
            "assessment": assessment,
            "severity": severity,
            "hr_used": latest_hr,
            "sbp_used": latest_sbp,
            "interpretation": f"SI = {round(si, 2)} ({assessment})"
        }
        
    except Exception as e:
        return {"error": f"Error calculating shock index: {str(e)}"}

@tool
def calculate_mean_arterial_pressure(events: Union[List[Dict], str]) -> dict:
    """
    Calculates Mean Arterial Pressure (MAP) from signal events.
    MAP = (SBP + 2*DBP) / 3
    Normal: >65 mmHg, Low Perfusion Warning: <65 mmHg

    Args:
        events: A list of dictionaries or JSON string representing signal alerts 
                (e.g., [{'category': 'SBP', 'signal_value': 120}, {'category': 'DBP', 'signal_value': 80}])

    Returns:
        A dictionary containing the MAP and clinical assessment.
    """
    try:
        # Handle string input (JSON)
        if isinstance(events, str):
            events = json.loads(events)
            
        if not isinstance(events, list):
            return {"error": "Events must be a list of dictionaries"}
            
        latest_sbp = None
        latest_dbp = None

        for event in reversed(events):
            if not isinstance(event, dict):
                continue
                
            category = event.get('category', '').upper()
            signal_value = event.get('signal_value')
            
            if category == 'SBP' and latest_sbp is None and signal_value is not None:
                latest_sbp = float(signal_value)
            elif category == 'DBP' and latest_dbp is None and signal_value is not None:
                latest_dbp = float(signal_value)
                
            if latest_sbp is not None and latest_dbp is not None:
                break
                
        if latest_sbp is None:
            return {"error": "No Systolic Blood Pressure (SBP) data found"}
        if latest_dbp is None:
            return {"error": "No Diastolic Blood Pressure (DBP) data found"}

        map_value = (latest_sbp + 2 * latest_dbp) / 3
        
        if map_value < 65:
            assessment = "Low Perfusion Warning"
            severity = "high"
        else:
            assessment = "Normal"
            severity = "negligible"
        
        return {
            "map": round(map_value, 1),
            "assessment": assessment,
            "severity": severity,
            "sbp_used": latest_sbp,
            "dbp_used": latest_dbp,
            "interpretation": f"MAP = {round(map_value, 1)} mmHg ({assessment})"
        }
        
    except Exception as e:
        return {"error": f"Error calculating MAP: {str(e)}"}

@tool
def check_sepsis_warning(events: Union[List[Dict], str]) -> dict:
    """
    Checks for early warning signs of sepsis based on fever and tachycardia.
    Criteria: Body Temperature >38.5°C AND Heart Rate >120 bpm

    Args:
        events: A list of dictionaries or JSON string representing signal events 
                (e.g., [{'category': 'BT', 'signal_value': 39.0}, {'category': 'HR', 'signal_value': 130}])

    Returns:
        A dictionary with sepsis warning assessment.
    """
    try:
        # Handle string input (JSON)
        if isinstance(events, str):
            events = json.loads(events)
            
        if not isinstance(events, list):
            return {"error": "Events must be a list of dictionaries"}
            
        latest_temp = None
        latest_hr = None

        for event in reversed(events):
            if not isinstance(event, dict):
                continue
                
            category = event.get('category', '').upper()
            signal_value = event.get('signal_value')
            
            if category == 'BT' and latest_temp is None and signal_value is not None:
                latest_temp = float(signal_value)
            elif category == 'HR' and latest_hr is None and signal_value is not None:
                latest_hr = float(signal_value)
                
            if latest_temp is not None and latest_hr is not None:
                break
                
        if latest_temp is None:
            return {"error": "No Body Temperature (BT) data found"}
        if latest_hr is None:
            return {"error": "No Heart Rate (HR) data found"}

        is_fever = latest_temp > 38.5
        is_tachycardic = latest_hr > 120

        if is_fever and is_tachycardic:
            return {
                "alert": "Sepsis Early Warning: High fever with disproportionate tachycardia detected",
                "is_critical": True,
                "severity": "urgent",
                "temperature": latest_temp,
                "heart_rate": latest_hr,
                "criteria_met": "Fever >38.5°C AND HR >120 bpm"
            }
        else:
            return {
                "alert": "No sepsis warning criteria met",
                "is_critical": False,
                "severity": "negligible",
                "temperature": latest_temp,
                "heart_rate": latest_hr,
                "criteria_status": f"Fever: {'Yes' if is_fever else 'No'}, Tachycardia: {'Yes' if is_tachycardic else 'No'}"
            }
            
    except Exception as e:
        return {"error": f"Error checking sepsis warning: {str(e)}"}

@tool
def check_cushings_triad(events: Union[List[Dict], str]) -> dict:
    """
    Checks for signs of Cushing's Triad (increased intracranial pressure).
    Criteria: Hypertension (SBP >180), Bradycardia (HR <60), Irregular respiration

    Args:
        events: A list of dictionaries or JSON string representing signal events

    Returns:
        A dictionary with Cushing's triad assessment.
    """
    try:
        # Handle string input (JSON)
        if isinstance(events, str):
            events = json.loads(events)
            
        if not isinstance(events, list):
            return {"error": "Events must be a list of dictionaries"}
            
        latest_sbp = None
        latest_hr = None
        is_respiration_irregular = False

        for event in reversed(events):
            if not isinstance(event, dict):
                continue
                
            category = event.get('category', '').upper()
            signal_value = event.get('signal_value')
            
            if category == 'SBP' and latest_sbp is None and signal_value is not None:
                latest_sbp = float(signal_value)
            elif category == 'HR' and latest_hr is None and signal_value is not None:
                latest_hr = float(signal_value)
            elif category in ['RESP', 'RR'] and event.get('status') == 'irregular':
                is_respiration_irregular = True
                
            if latest_sbp is not None and latest_hr is not None:
                break

        if latest_sbp is None:
            return {"error": "No Systolic Blood Pressure (SBP) data found"}
        if latest_hr is None:
            return {"error": "No Heart Rate (HR) data found"}

        is_hypertensive = latest_sbp > 180
        is_bradycardic = latest_hr < 60

        criteria_met = [is_hypertensive, is_bradycardic, is_respiration_irregular]
        
        if all(criteria_met):
            return {
                "alert": "Cushing's Triad Detected: Critical sign of increased intracranial pressure",
                "is_critical": True,
                "severity": "urgent",
                "sbp": latest_sbp,
                "heart_rate": latest_hr,
                "irregular_respiration": is_respiration_irregular,
                "criteria_met": "All three criteria met"
            }
        else:
            return {
                "alert": "Cushing's Triad not detected",
                "is_critical": False,
                "severity": "negligible",
                "sbp": latest_sbp,
                "heart_rate": latest_hr,
                "irregular_respiration": is_respiration_irregular,
                "criteria_status": f"Hypertension: {'Yes' if is_hypertensive else 'No'}, Bradycardia: {'Yes' if is_bradycardic else 'No'}, Irregular Resp: {'Yes' if is_respiration_irregular else 'No'}"
            }
            
    except Exception as e:
        return {"error": f"Error checking Cushing's triad: {str(e)}"}


# Test function for debugging
if __name__ == "__main__":
    test_events = [
        {'signal_value': 72.0, 'time_recorded': 24.2807, 'caseid': 2626, 'category': 'HR', 'reason': 'z_score_mild:2.20'}, 
        {'signal_value': 120.0, 'time_recorded': 25.0, 'caseid': 2626, 'category': 'SBP', 'reason': 'test'},
        {'signal_value': 80.0, 'time_recorded': 25.0, 'caseid': 2626, 'category': 'DBP', 'reason': 'test'}
    ]
    
    print("Testing tools:")
    print("Shock Index:", calculate_shock_index(test_events))
    print("MAP:", calculate_mean_arterial_pressure(test_events))
    print("Sepsis Warning:", check_sepsis_warning(test_events))
    print("Cushing's Triad:", check_cushings_triad(test_events))