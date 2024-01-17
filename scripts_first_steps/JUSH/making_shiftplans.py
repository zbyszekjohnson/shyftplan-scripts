import pandas as pd
import json
import requests
import time
import pprint
import os

# STAŁE
user_email = os.environ['SHYFTPLAN_EMAIL']
authentication_token = os.environ['SHYFTPLAN_JUSH_API_KEY']
# DATY OD KIEDY DO KIEDY MA BYĆ SHIFTPLAN WEEK
shiftplan_start_date = "15.1.2024"
shiftplan_end_date = "21.1.2024"


def api_call_load_locations(user_email, authentication_token):

    url = f"https://shyftplan.com/api/v1/locations?user_email={user_email}&authentication_token={authentication_token}&company_id=50272"
    headers = {"accept": "application/json"}
    response = requests.get(url, headers=headers)
    return response.json()


def api_call_post_shiftplan(user_email, authentication_token, location_id, location_name, shiftplan_start_date, shiftplan_end_date):
    url = "https://shyftplan.com/api/v1/shiftplans"

    payload = {
        "user_email": user_email,
        "authentication_token": authentication_token,
        "location_id": location_id,
        "name": location_name,
        "starts_at": shiftplan_start_date,
        "ends_at": shiftplan_end_date,
    }
    headers = {
        "accept": "application/json",
        "content-type": "application/json"
    }
    response = requests.post(url, json=payload, headers=headers)
    time.sleep(0.5)


# STWORZENIE SŁOWNIKA "location_id: location_name"
locations_raw = api_call_load_locations(user_email, authentication_token)
locations_ids = {location["id"]: location["name"].split(" (")[0]
                 for location in locations_raw["items"]
                 if location["name"].split(" (")[0] not in ["LOKALIZACJA TESTOWA", "Grochowskiego 5 [Piaseczno]", "Wilcza 1", "Gdyńska 3", "Fieldorfa 2", "Arkońska 6", "Rakoczego 19", "Czarnieckiego 6", "Gliwicka 2", "Tarnowiecka 13", "Puławska 17", "Sukiennicza 8", "Mogilska 21", "Wrocławska 53B", "Solidarności 155", "Herbu Janina 5", "Rydygiera 13", "Dolna 41", "Lekka 3", "Elektryczna 2", "Międzynarodowa 42"]}

for location_id, location_name in locations_ids.items():
    api_call_post_shiftplan(user_email, authentication_token, location_id,
                            location_name, shiftplan_start_date, shiftplan_end_date)

pprint.pprint(locations_raw)
pprint.pprint(locations_ids)
