import pandas as pd
import datetime
import requests
import os
import time
import pprint

# Load CSV file
data = pd.read_csv("riders_shifts.csv")

url = "https://shyftplan.com/api/v1/staff_shifts"
user_email = os.environ['SHYFTPLAN_EMAIL']
authentication_token = os.environ['SHYFTPLAN_JUSH_API_KEY']
# UZYWAJ TEJ DATY DO WYFILTORWANIA OSTATNICH TYGODNI GRAFIKOWYCH np. jak stworzyłeś nowe grafiki 18.07 to ustaw na dzien przed aby do skryptu uzywalo tylko najnowyszch
created_after = "07.09.2023"

# Process data and create dictionary
shift_preferences = {}


# UZYSKANIE DANYCH O LOCATION_POSITION_IDs
def api_call_locations_positions(user_email, authentication_token):
    url = f"https://shyftplan.com/api/v1/locations_positions?user_email={user_email}&authentication_token={authentication_token}&page=1&per_page=1000"
    headers = {"accept": "application/json"}
    response = requests.get(url, headers=headers)
    return response.json()


# UZYSKANIE DANYCH o WOLNYCH ZMIANACH
def api_call_shifts(created_after):
    shifts_list = []

    for page in range(1, 21):
        url = f"https://shyftplan.com/api/v1/shifts?user_email=krystian.solopa%40lite.tech&authentication_token={authentication_token}&company_id=50272&page={page}&per_page=200&created_after={created_after}&only_open=true"
        headers = {"accept": "application/json"}
        response = requests.get(url, headers=headers)
        shifts = response.json()
        # assuming 'items' contains the shifts
        shifts_list.extend(shifts["items"])

    return shifts_list


# PRZYPISYWANIE RIDERÓW DO ZMIAN
def api_call(employment_id, shift_id):
    payload = {
        "assign_to_connected": False,
        "ignore_conflicts": "false",
        "user_email": "krystian.solopa@lite.tech",
        "authentication_token": authentication_token,
        "company_id": 50272,
        "shift_id": shift_id,
        "employment_id": employment_id,
    }
    headers = {"accept": "application/json",
               "content-type": "application/json"}
    response = requests.post(url, json=payload, headers=headers)
    print(response, employment_id)
    time.sleep(0.5)


# UZYSKIWANIE DANYCH o LOKALIZACJACH
def api_call_locations(user_email, authentication_token):
    url = f"https://shyftplan.com/api/v1/locations?user_email={user_email}&authentication_token={authentication_token}"
    headers = {"accept": "application/json"}
    response = requests.get(url, headers=headers)
    return response.json()


# UZYSKIWANIE DANYCH o "shyftplan_id - tygodniach grafikowych"
def api_call_shiftplans(user_email, authentication_token, created_after):
    url = f"https://shyftplan.com/api/v1/shiftplans?user_email={user_email}&authentication_token={authentication_token}&created_after={created_after}&order_key=id&order_dir=desc"
    headers = {"accept": "application/json"}
    response = requests.get(url, headers=headers)
    return response.json()


# Zrobienie słownika Location_Position_IDS
locations_positions = api_call_locations_positions(
    user_email, authentication_token)
position_ids = {}
for item in locations_positions["items"]:
    position_name = item["position_name"]
    position_id = item["id"]
    if position_name in position_ids:
        position_ids[position_name].append(position_id)
    else:
        position_ids[position_name] = [position_id]

# Define a week starting from a specific date
week = {
    "poniedziałek": datetime.date(2023, 9, 11),
    "wtorek": datetime.date(2023, 9, 12),
    "środa": datetime.date(2023, 9, 13),
    "czwartek": datetime.date(2023, 9, 14),
    "piątek": datetime.date(2023, 9, 15),
    "sobota": datetime.date(2023, 9, 16),
    "niedziela": datetime.date(2023, 9, 17),
}

# STWORZENIE SHIFT_PREFERENCES
for index, row in data.iterrows():
    employment_id = row["employment_id"]
    place = row["place"]
    shift_preferences[(place, employment_id)] = {}
    for day in week.keys():
        for czas in ["7:00-11:00", "11:00-15:00", "15:00-19:00", "19:00-23:00"]:
            # Check if the preference value is 0 or 1
            if row[f"{czas} ({day})"] in [0, 1]:
                # Split the time into start and end
                start_time, end_time = czas.split("-")

                # Dodanie ':00' do czasów, które nie zawierają minut
                if ":" not in start_time:
                    start_time += ":00"
                if ":" not in end_time:
                    end_time += ":00"

                # Convert start and end time to datetime format
                start_time = datetime.datetime.strptime(
                    start_time, "%H:%M").time()
                end_time = datetime.datetime.strptime(end_time, "%H:%M").time()

                # Combine date and time
                start_datetime = datetime.datetime.combine(
                    week[day],
                    start_time,
                    tzinfo=datetime.timezone(datetime.timedelta(hours=2)),
                )
                if end_time < start_time:
                    end_datetime = datetime.datetime.combine(
                        week[day] + datetime.timedelta(days=1),
                        end_time,
                        tzinfo=datetime.timezone(datetime.timedelta(hours=2)),
                    )
                else:
                    end_datetime = datetime.datetime.combine(
                        week[day],
                        end_time,
                        tzinfo=datetime.timezone(datetime.timedelta(hours=2)),
                    )

                # Convert to ISO format and remove seconds
                start_datetime = (
                    start_datetime.isoformat()[:16]
                    + ":00.000"
                    + start_datetime.isoformat()[19:]
                )
                end_datetime = (
                    end_datetime.isoformat()[:16]
                    + ":00.000"
                    + end_datetime.isoformat()[19:]
                )

                # Update the dictionary with the actual preference value (0 or 1)
                shift_preferences[(place, employment_id)][
                    f"{start_datetime}x{end_datetime}"
                ] = row[f"{czas} ({day})"]

# UZYSKANIE ID WSZYSTKICH LOKALIZACJI
locations = api_call_locations(user_email, authentication_token)
location_ids = {location["name"]: location["id"]
                for location in locations["items"]}

# Create a new dictionary with location IDs instead of names
new_shift_preferences = {}
for key, value in shift_preferences.items():
    place, employment_id = key
    for location_name, location_id in location_ids.items():
        if place in location_name:
            # Replace place with its corresponding ID
            new_shift_preferences[(location_id, employment_id)] = value
            # Stop the loop once a match is found
            break
# Replace the old dictionary with the new one
shift_preferences = new_shift_preferences

# Get all shiftplan weeks
all_shiftplans = api_call_shiftplans(
    user_email, authentication_token, created_after)

# Create a dictionary that maps each location_id to the corresponding shiftplan_id
location_to_shiftplan = {}
for shiftplan in all_shiftplans["items"]:
    location_id = shiftplan["location_id"]
    shiftplan_id = shiftplan["id"]
    if location_id not in location_to_shiftplan:
        location_to_shiftplan[location_id] = shiftplan_id

# Create a new dictionary with shiftplan_ids instead of location_ids
new_shift_preferences = {}
for key, value in shift_preferences.items():
    location_id, employment_id = key
    if location_id in location_to_shiftplan:
        # Replace location_id with its corresponding shiftplan_id
        new_shift_preferences[
            (location_to_shiftplan[location_id], employment_id)
        ] = value

# Replace the old dictionary with the new one
shift_preferences = new_shift_preferences


# Get all open shifts
all_shifts = api_call_shifts(created_after)

# Create a dictionary for all open shifts
open_shifts = {}
for shift in all_shifts:
    shift_id = shift["id"]
    starts_at = shift["starts_at"]
    ends_at = shift["ends_at"]
    workers = shift["workers"]
    shiftplan_id = shift["shiftplan_id"]
    shift_type = shift["manager_note"]
    # assuming this is the correct field
    open_shifts[shift_id] = {
        "starts_at": starts_at,
        "ends_at": ends_at,
        "workers": workers,
        "shiftplan_id": shiftplan_id,
        "shift_type": shift_type,
    }

pprint.pprint(open_shifts)

# Create a dictionary to keep track of the remaining shifts of each type
remaining_shifts = {"LITE": []}  # Add a 'LITE' shift category

# Separate all open shifts into their respective categories and store their IDs
for shift_id, shift_info in open_shifts.items():
    if shift_info["shift_type"] == "LITE":  # Only consider 'LITE' shifts
        remaining_shifts[shift_info["shift_type"]].append(shift_id)

print(len(open_shifts))
print(len(remaining_shifts))

# Iterate over each employee's shift preferences.
# Each shift declaration will be checked if it can be assigned.
# FIRST ITERATION THROUGH his ID
for key, value in shift_preferences.items():
    shiftplan_id, employment_id = key
    # SECOND ITERATION already through all shift declarations of a given Rider
    for shift_time, preference in value.items():
        if preference == 1:
            # If Preference == 1 (always) then we split his datetime to make matching with what we get from Api
            start_datetime, end_datetime = (
                shift_time.split("x")[0],
                shift_time.split("x")[1],
            )

            # Always consider 'LITE' shifts
            for shift_id in remaining_shifts["LITE"]:
                shift_info = open_shifts[shift_id]
                # Check if the shiftplan_id, start_datetime, end_datetime match
                if (
                    shift_info["shiftplan_id"] == shiftplan_id
                    and shift_info["starts_at"] == start_datetime
                    and shift_info["ends_at"] == end_datetime
                ):
                    # Assign the employee to the shift
                    api_call(employment_id, shift_id)
                    # Decrement the number of workers needed for the shift
                    shift_info["workers"] -= 1
                    # If no more workers are needed for the shift, remove it from the remaining shifts
                    if shift_info["workers"] == 0:
                        remaining_shifts["LITE"].remove(shift_id)
                    break
