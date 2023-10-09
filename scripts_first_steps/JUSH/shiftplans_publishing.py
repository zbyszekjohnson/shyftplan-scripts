import pandas as pd
import requests
import os

url = "https://shyftplan.com/api/v1/staff_shifts"
user_email = os.environ['SHYFTPLAN_EMAIL']
authentication_token = os.environ['SHYFTPLAN_JUSH_API_KEY']
# UZYWAJ TEJ DATY DO WYFILTORWANIA OSTATNICH TYGODNI GRAFIKOWYCH np. jak stworzyłeś nowe grafiki 18.07 to ustaw na dzien przed aby do skryptu uzywalo tylko najnowyszch
created_after = "5.10.2023"


def api_call_shiftplans(user_email, authentication_token, created_after):
    url = f"https://shyftplan.com/api/v1/shiftplans?user_email={user_email}&authentication_token={authentication_token}&created_after={created_after}&order_key=id&order_dir=desc"
    headers = {"accept": "application/json"}
    response = requests.get(url, headers=headers)
    return response.json()


shiftplans_raw = api_call_shiftplans(
    user_email, authentication_token, created_after)
shiftplans = {shiftplan["id"]: shiftplan["name"]
              for shiftplan in shiftplans_raw["items"]}


def api_call_shiftplan_publish(id, user_email, authentication_token):
    import requests

    url = f"https://shyftplan.com/api/v1/shiftplans/{id}/publish"

    payload = {
        "publish_with_email": False,
        "send_assigned_shifts": False,
        "send_open_shifts": False,
        "send_message": False,
        "message": None,
        "user_email": user_email,
        "authentication_token": authentication_token,
        "company_id": 50272
    }
    headers = {
        "accept": "application/json",
        "content-type": "application/json"
    }
    response = requests.post(url, json=payload, headers=headers)

    print(response.text)

for id in shiftplans:   
    api_call_shiftplan_publish(id, user_email, authentication_token)
    print(id)