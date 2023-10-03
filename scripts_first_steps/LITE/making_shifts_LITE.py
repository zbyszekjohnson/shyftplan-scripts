import pandas as pd
import requests
import datetime
import logging
import time
import json
import re
import pprint
import os

logging.basicConfig(level=logging.INFO, filename="errors.log", filemode="w")

# Zmienne
url = "https://shyftplan.com/api/v1/shifts"
user_email = os.environ['SHYFTPLAN_EMAIL']
authentication_token = os.environ['SHYFTPLAN_JUSH_API_KEY']

# DO WYFILTROWANIA OSTATNICH SHYFTPLANÓW, np. jeśli stworzyłeś je 18.08.2023 - to wpisz tą datę lub dzień wczesniej :)
created_after = "28.09.2023"

# Data na jaka wrzucasz zmiany :)
today = datetime.date(2023, 10, 8)


def api_call_locations_position_id():
    url = f"https://shyftplan.com/api/v1/locations_positions?user_email={user_email}&authentication_token={authentication_token}&page=1&per_page=1000"
    headers = {"accept": "application/json"}
    response = requests.get(url, headers=headers)
    data = response.json()
    return data.get("items", [])


def clean_DS_name(ds_name):
    return re.sub(r"\s+\(.*\)", "", ds_name)


def api_call_get_shiftplans():

    url = f"https://shyftplan.com/api/v1/shiftplans?user_email={user_email}&authentication_token={authentication_token}&created_after={created_after}&order_key=id&order_dir=desc"
    headers = {"accept": "application/json"}
    response = requests.get(url, headers=headers)
    data = response.json()
    return data.get("items", [])


def api_call_post_shift(shiftplan_id, starts_at, ends_at, workers, locations_position_id, note):
    payload = {
        "can_evaluate": True,
        "untimed": False,
        "ignore_conflicts": "true",
        "user_email": user_email,
        "authentication_token": authentication_token,
        "starts_at": starts_at,
        "ends_at": ends_at,
        "workers": int(workers),
        "shiftplan_id": int(shiftplan_id),
        "locations_position_id": int(locations_position_id),
        "auto_accept": "enforced",
        "untimed_break_time": 0,
        "manager_note": note,
    }
    headers = {"accept": "application/json",
               "content-type": "application/json"}

    time.sleep(0.5)
    # taka podstawa do logów, trzeba to rozwinąć
    try:
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()

    except requests.exceptions.RequestException as e:
        logging.error(f"Błąd żądania: {e}")
        return

    if response.status_code == 201:
        print(f"Status odpowiedzi: {response.status_code}, {response.reason}")
    else:
        with open("errors.json", "a") as f:
            error_details = {
                "status_code": response.status_code,
                "reason": response.reason,
                "shiftplan_id": shiftplan_id,
                "starts_at": starts_at,
                "ends_at": ends_at,
                "workers": workers,
                "locations_position_id": locations_position_id,
            }
            f.write(json.dumps(error_details) + "\n")

    logging.info(
        f"Status odpowiedzi: {response.status_code}, powód: {response.reason}")


# SŁOWNIK SHIFTPLANÓW
shiftplans_dict_raw = api_call_get_shiftplans()
shiftplans_dict = {}
for item in shiftplans_dict_raw:
    shiftplan_id = item["id"]
    name = item["name"]
    shiftplans_dict[shiftplan_id] = {
        "DS_name": name
    }
shiftplans_df = pd.DataFrame(shiftplans_dict).T.reset_index()
shiftplans_df.columns = ['shiftplan_id', 'DS_name']

# SŁOWNIK LOCATIONS_POSITION_ID
locations_position_id_dict_raw = api_call_locations_position_id()
locations_position_id_dict = {}
for item in locations_position_id_dict_raw:
    locations_position_id = item["id"]
    vehicle = item["position_name"]
    DS_name = item["location_name"]
    if vehicle in ["LITE"]:
        locations_position_id_dict[locations_position_id] = {
            "locations_position_id": locations_position_id,
            "vehicle": vehicle,
            "DS_name": clean_DS_name(DS_name)
        }

df_positions = pd.DataFrame.from_dict(
    locations_position_id_dict, orient="index")
df_positions = df_positions.merge(shiftplans_df, on='DS_name', how='left')
df_positions = df_positions.dropna(subset=['shiftplan_id'])
df_positions['shiftplan_id'] = df_positions['shiftplan_id'].astype('Int64')

pprint.pprint(df_positions)
def main():
    # Wczytujemy pliki CSV
    df_shifts = pd.read_csv("shifts_lite.csv")

    # Modyfikacja df_shifts, żeby wszystko było w jednym wierszu
    df_shifts = df_shifts.melt(
        id_vars="place", var_name="shift_time", value_name="workers"
    )

    # Iterujemy przez każdą zmianę, definiujemy zmienne, i pomijamy wszystko gdzie workers = 0
    # np. place = Szarych Szeregów 11, shift_time = 7-11, workers = 2
    for index, row in df_shifts.iterrows():
        place = row["place"]
        shift_time = row["shift_time"]
        workers = row["workers"]

        if workers == 0:
            continue

        starts_at, ends_at = shift_time.split("-")
        if ":" not in starts_at:
            starts_at += ":00"
        if ":" not in ends_at:
            ends_at += ":00"
        start_time = datetime.datetime.strptime(starts_at, "%H:%M")
        end_time = datetime.datetime.strptime(ends_at, "%H:%M")

        # Zróbmy z 7:00 ---> 2023-06-18 07:00:00+02:00 itd.
        start_datetime = datetime.datetime.combine(
            today,
            start_time.time(),
            tzinfo=datetime.timezone(datetime.timedelta(hours=2)),
        )

        # Jeśli zmiana kończy się po północy, dodajemy 1 dzień do daty końca
        if end_time < start_time:
            end_datetime = datetime.datetime.combine(
                today + datetime.timedelta(days=1),
                end_time.time(),
                tzinfo=datetime.timezone(datetime.timedelta(hours=2)),
            )
        else:
            end_datetime = datetime.datetime.combine(
                today,
                end_time.time(),
                tzinfo=datetime.timezone(datetime.timedelta(hours=2)),
            )

        # Zmieniamy z 2023-06-18 07:00:00+02:00 ---> 2023-06-18T19:00:00+02:00 w formacie ISO itd.
        starts_at_iso = start_datetime.isoformat()
        ends_at_iso = end_datetime.isoformat()

        # Zmieniamy z 2023-06-18T19:00:00+02:00 ---> 2023-06-18T19:00+02:00 (usuwamy sekundy) itd.
        starts_at_iso = starts_at_iso[:16] + starts_at_iso[19:]
        ends_at_iso = ends_at_iso[:16] + ends_at_iso[19:]

        # Get position info for the current location
        # #TUTAJ ZMODYFIKOWAĆ TO MOCNO --- ABY POBIERAŁO Z API CALL INFO O ZMIANACH LITE RRIDERA I LUTA zeby wgrywac shift.csv
        position_info = df_positions[(df_positions["DS_name"] == place)]

        # Get locations_position_id and shiftplan_id
        locations_position_id = position_info["locations_position_id"].values[0]
        shiftplan_id = position_info["shiftplan_id"].values[0]

        # Ustawienie kolejności. Ustawiamy zmienna vehicle na jedna z 3 elementow tej tablicy - to da nam gwarancje kolejnosci wrzucania zmian z priorytetem
        # poniewaz zmienna vehicle bedzie potem uzywana w kazdej operacji dotyczacej co wrzucamy i dlaczego wrzucamy :)

        # Przypisujemy pracownika do tego pojazdu i aktualizujemy dostępność
        api_call_post_shift(
            shiftplan_id=shiftplan_id,
            starts_at=starts_at_iso,
            ends_at=ends_at_iso,
            workers=workers,
            locations_position_id=locations_position_id,
            note="LITE",
        )

        # break w przypadku gdy juz nie mamy co przypisywać, przerywamy pętle dla tej iteracji
        if workers == 0:
            break


if __name__ == "__main__":
    main()
