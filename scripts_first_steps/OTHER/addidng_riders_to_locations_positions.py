import requests
import pandas as pd
import time
import pprint
import os

user_email = os.environ['SHYFTPLAN_EMAIL']
authentication_token = os.environ['SHYFTPLAN_JUSH_API_KEY']


def rider_update(employment_id, locations_positon_id):


    url = "https://shyftplan.com/api/v1/employments_positions"

    payload = {
        "user_email": "krystian.solopa@lite.tech",
        "authentication_token": authentication_token,
        "employment_id": int(employment_id),
        "locations_position_id": int(locations_positon_id),
        "company_id": 50272
    }
    headers = {
        "accept": "application/json",
        "content-type": "application/json"
    }

    response = requests.post(url, json=payload, headers=headers)
    print(response)
    return response.json()

df_positions = pd.read_csv("riders.csv")
print(df_positions)
pprint.pprint(df_positions)
for index, row in df_positions.iterrows():
    rider_update(row['employment_id'],row['locations_positon_id'])

