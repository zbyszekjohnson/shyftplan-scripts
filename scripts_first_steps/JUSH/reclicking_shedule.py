import requests
import pprint
import time
import os

# UZYWAJ TEJ DATY DO WYFILTORWANIA OSTATNICH TYGODNI GRAFIKOWYCH np. jak stworzyłeś nowe grafiki 18.07 to ustaw na dzien przed aby do skryptu uzywalo tylko najnowyszch
created_after = "2023-09-07"
user_email = os.environ['SHYFTPLAN_EMAIL']
authentication_token = os.environ['SHYFTPLAN_JUSH_API_KEY']
company_id = "50272"
shyftplan_id = "494278"

def api_call_locations_position_id_list():
    url = f"https://shyftplan.com/api/v1/locations_positions?user_email={user_email}&authentication_token={authentication_token}&page=1&per_page=1000"
    headers = {"accept": "application/json"}
    response = requests.get(url, headers=headers)
    data = response.json()
    return data.get("items", [])


def api_call_shift_make(starts_at, ends_at, workers, shiftplan_id, locations_position_id):
    url = "https://shyftplan.com/api/v1/shifts"
    payload = {
        "can_evaluate": True,
        "untimed": False,
        "ignore_conflicts": "true",
        "user_email": user_email,
        "authentication_token": authentication_token,
        "starts_at": starts_at,
        "ends_at": ends_at,
        "workers": workers,
        "shiftplan_id": shiftplan_id,
        "locations_position_id": locations_position_id,
        "break_time": 0
    }
    headers = {
        "accept": "application/json",
        "content-type": "application/json"
    }

    response = requests.post(url, json=payload, headers=headers)
    print(response, "shiftplan id:", shiftplan_id, "locations_position_id: ",
          locations_position_id, "starts_at: ", starts_at)
    print("Shift created ", response.status_code)
    time.sleep(0.5)


def api_call_shift_delete(shift_id):

    url = f"https://shyftplan.com/api/v1/shifts/{shift_id}?user_email={user_email}&authentication_token={authentication_token}&company_id={company_id}&delete_connected=false"
    headers = {"accept": "application/json"}
    response = requests.delete(url, headers=headers)
    print("Shift Deleted ", response.status_code)
    print(response)
    time.sleep(0.5)


def api_call_shifts_list(created_after):
    shifts_list = []
    per_page = 1000
    page = 1

    url = f"https://shyftplan.com/api/v1/shifts?user_email={user_email}&authentication_token={authentication_token}&company_id={company_id}&page={page}&per_page={per_page}&created_after={created_after}&only_open=true&extended_info=true"
    headers = {"accept": "application/json"}
    response = requests.get(url, headers=headers)
    shifts = response.json()
    total_items = shifts["total"]
    # Calculate total number of pages
    total_pages = (total_items + per_page - 1) // per_page

    shifts_list.extend(shifts["items"])

    # Start from page 2, as page 1 is already fetched
    for page in range(2, total_pages + 1):
        url = f"https://shyftplan.com/api/v1/shifts?user_email={user_email}&authentication_token={authentication_token}&company_id={company_id}&page={page}&per_page={per_page}&created_after={created_after}&only_open=true&extended_info=true"
        response = requests.get(url, headers=headers)
        shifts = response.json()
        shifts_list.extend(shifts["items"])

    return shifts_list


def api_call_shift_update(shift_id, workers_count):
    url = f"https://shyftplan.com/api/v1/shifts/{shift_id}"

    payload = {
        "user_email": user_email,
        "authentication_token": authentication_token,
        "company_id": company_id,
        "workers": workers_count
    }
    headers = {
        "accept": "application/json",
        "content-type": "application/json"
    }
    response = requests.patch(url, json=payload, headers=headers)
    print("Shift Updated ", response.status_code)
    print(response)
    time.sleep(0.5)


def find_correspondind_id(position_name, location_name, loc_pos_data):
    position_mapping = {
        "Zaufany Rider - Warszawa": "Rider - Warszawa",
        "Zaufany E-bike - Warszawa": "E-bike - Warszawa",
        "e-Skuter Jush! - Zaufany Rider - Warszawa": "Skuter - Warszawa",
        "Zaufany Rider - Gdańsk": "Rider - Gdańsk",
        "Zaufany E-bike - Gdańsk": "E-bike - Gdańsk",
        "e-Skuter Jush! - Zaufany Rider - Gdańsk": "Skuter - Gdańsk",
        "Zaufany Rider - Kraków": "Rider - Kraków",
        "Zaufany E-bike - Kraków": "E-bike - Kraków",
        "e-Skuter Jush! - Zaufany Rider - Kraków": "Skuter - Kraków",
        "e-Skuter Jush! - Zaufany Rider - Katowice": "Skutery - Katowice",
        "Zaufany Rider - Katowice": "Skutery - Katowice",
        "Zaufany Rider - Poznań": "Skuter - Poznań",
        "Zaufany E-bike - Poznań": "Skuter - Poznań",
        "e-Skuter Jush! - Zaufany Rider - Poznań": "Skuter - Poznań"
    }
    # DLA  "e-Skuter Jush! - Zaufany Rider - Poznań"  ------->  "Skuter - Poznań"
    target_position_name = position_mapping.get(position_name)
    for loc_id, item in loc_pos_data.items():
        if item['position_name'] == target_position_name and item['location_name'] == location_name:
            return loc_id
    return None


locations_position_id_dict_raw = api_call_locations_position_id_list()
locations_position_id_dict = {}
for loc_pos_id in locations_position_id_dict_raw:    # type: ignore
    locations_position_id = loc_pos_id["id"]
    position_name = loc_pos_id["position_name"]
    location_name = loc_pos_id["location_name"]

    locations_position_id_dict[locations_position_id] = {
        "position_name": position_name,
        "location_name": location_name
    }

open_shifts_raw = api_call_shifts_list(created_after)
open_shifts = {}
for shift in open_shifts_raw:
    shift_id = shift["id"]
    starts_at = shift["starts_at"]
    ends_at = shift["ends_at"]
    workers_max = shift["workers"]
    workers_count = shift["staff_shifts_count"]
    shiftplan_id = shift["shiftplan_id"]
    shift_type = shift["manager_note"]
    locations_position_id = shift["locations_position_id"]
    open_shifts[shift_id] = {
        "starts_at": starts_at,
        "ends_at": ends_at,
        "workers_max": workers_max,
        "workers_count": workers_count,
        "shiftplan_id": shiftplan_id,
        "shift_type": shift_type,
        "locations_position_id": locations_position_id,
    }

print(len(open_shifts))

for shift in open_shifts:
    if not open_shifts[shift]["shift_type"]:
        continue
    workers_count = int(open_shifts[shift]["workers_count"])
    workers_max = int(open_shifts[shift]["workers_max"])
    shiftplan_id = int(open_shifts[shift]["shiftplan_id"])
    starts_at = open_shifts[shift]["starts_at"]
    ends_at = open_shifts[shift]["ends_at"]
    shift_type = open_shifts[shift]["shift_type"]
    locations_position_id = open_shifts[shift]["locations_position_id"]
    position_name = locations_position_id_dict[open_shifts[shift]
                                               ["locations_position_id"]]['position_name']
    location_name = locations_position_id_dict[open_shifts[shift]
                                               ["locations_position_id"]]['location_name']
    changed_locations_position_id = find_correspondind_id(
        position_name, location_name, locations_position_id_dict)

    if workers_count == workers_max:
        continue
    elif workers_count == 0 and shift_type in ["Zaufany E-bike", "Zaufany Skuter", "Zaufany Rider"]:
        x = api_call_shift_delete(shift)  # type: ignore
        api_call_shift_make(starts_at, ends_at, workers_max,
                            shiftplan_id, changed_locations_position_id)  # type: ignore

    elif workers_count < workers_max and shift_type in ["Zaufany E-bike", "Zaufany Skuter", "Zaufany Rider"]:
        workers_diff = workers_max - workers_count
        api_call_shift_update(shift, workers_count)

        api_call_shift_make(starts_at, ends_at, workers_diff,
                            shiftplan_id, changed_locations_position_id)  # type: ignore
