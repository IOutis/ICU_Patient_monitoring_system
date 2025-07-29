import os
import pandas as pd

def load_excel(filename: str) -> pd.DataFrame:
    """
    Loads an Excel file from the data_preparation directory (project-root relative).
    """
    root = os.path.abspath(os.path.join(__file__, '../../..'))  # adjust as per file location
    file_path = os.path.join(root, 'data_preparation/data', filename)

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")
    
    return pd.read_excel(file_path)

# Usage
patient_info = load_excel('cases.xlsx')
signals_info = load_excel('track_list.xlsx')
lab_info = load_excel('vitaldb_labs.xlsx')


# I have generated 30 random case ids from 1 to 6388. This is to ensure that I get random patients instead of sequnetially.
random_list_of_caseids = [3638, 1209, 1087, 1903, 5540, 5211, 2422, 2327, 1952, 634, 4721, 4658, 6234, 2880, 2340, 1488, 3519, 2191, 2626, 5248, 1032, 5841, 4978, 6335, 2259, 5337, 3984, 2724, 609, 5107]

#now let's filter the dataframes accordingly
filtered_patient_info = patient_info[patient_info['caseid'].isin(random_list_of_caseids)]
filtered_signals_info = signals_info[signals_info['caseid'].isin(random_list_of_caseids)]
filtered_lab_info = lab_info[lab_info['caseid'].isin(random_list_of_caseids)]


#Instead of monitoring all the signals, I have selected most important signals of these 30 patients. Some signals given below have same purpose but different machine used to measure that signal example : Heart Rate - Solar8000/HR or CardioQ/HR
selected_signals_dict = {
    "Heart Rate": [
        "Solar8000/HR",
        "CardioQ/HR",
        "Solar8000/PLETH_HR"
    ],
    "Arterial Systolic Pressure": [
        "Solar8000/ART_SBP"
    ],
    "Arterial Diastolic Pressure": [
        "Solar8000/ART_DBP"
    ],
    "Mean Arterial Pressure": [
        "Solar8000/ART_MBP",
        "EV1000/ART_MBP"
    ],
    "Respiratory Rate": [
        "Solar8000/RR",
        "Solar8000/RR_CO2",
        "Primus/RR_CO2",
        "Solar8000/VENT_RR"
    ],
    "Oxygen Saturation": [
        "Solar8000/PLETH_SPO2"
    ],
    "Body Temperature": [
        "Solar8000/BT"
    ],
    "ECG Lead II": [
        "SNUADC/ECG_II"
    ],
    "End-Tidal CO2": [
        "Solar8000/ETCO2",
        "Primus/ETCO2"
    ],
    "Central Venous Pressure": [
        "SNUADC/CVP",
        "Solar8000/CVP",
        "EV1000/CVP"
    ]
}


# Let's filter the filtered_signals_info based on the selected signals
final_signals_info = []
missing_signals = []

case_ids = filtered_signals_info['caseid'].unique()

for caseid in case_ids:
    case_df = filtered_signals_info[filtered_signals_info['caseid'] == caseid]
    for category, possible_names in selected_signals_dict.items():
        match = case_df[case_df['tname'].isin(possible_names)]
        if not match.empty:
            best_row = match.iloc[0]
            final_signals_info.append({
                'caseid': caseid,
                'category': category,
                'tname': best_row['tname'],
                'tid': best_row['tid']
            })
        else:
            missing_signals.append((caseid, category))

final_signals_info_df = pd.DataFrame(final_signals_info)

# Display missing signals (optional)
for caseid, category in missing_signals:
    print(f"[MISSING] caseid: {caseid}, signal: {category}")

print("Total No. Of missing signals")
print(len(missing_signals))
    
    
print("Final Tracks list : ")
print(final_signals_info_df.groupby('caseid')['tname'].nunique())
final_signals_info_df.to_excel("final_signals_info.xlsx", index=False)
filtered_patient_info.to_excel('filtered_patient_info.xlsx',index=False)
filtered_lab_info.to_excel('filtered_lab_info.xlsx',index=False)