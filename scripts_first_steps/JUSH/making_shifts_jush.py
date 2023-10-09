import pandas as pd
import requests
import datetime
import logging
import time
import json
import os
import re


logging.basicConfig(level=logging.INFO, filename="errors.log", filemode="w")

# Zmienne
url = "https://shyftplan.com/api/v1/shifts"
user_email = os.environ['SHYFTPLAN_EMAIL']
authentication_token = os.environ['SHYFTPLAN_JUSH_API_KEY']
pattern = r"^(e-Skuter Jush! - Zaufany Rider|Zaufany E-bike|Zaufany Rider)( - [A-Za-ząęśćółńżź]+)?$"

# DO WYFILTROWANIA OSTATNICH SHYFTPLANÓW, np. jeśli stworzyłeś je 18.08.2023 - to wpisz tą datę lub dzień wczesniej :)
created_after = "05.10.2023"
# Dzień dzisiejszy dla testów - musimy zmodyfikować to na różne dni
today = datetime.date(2023, 10, 15)


# Funkcja do API Call
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
        print(
            f"Response status: {response.status_code}, {response.reason}. This is shiftplan_id {shiftplan_id}")

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
    if vehicle in ["e-Skuter Jush! - Zaufany Rider - Warszawa",
                   "e-Skuter Jush! - Zaufany Rider - Poznań",
                   "e-Skuter Jush! - Zaufany Rider - Kraków",
                   "e-Skuter Jush! - Zaufany Rider - Katowice",
                   "e-Skuter Jush! - Zaufany Rider - Gdańsk",
                   "Zaufany E-bike - Warszawa",
                   "Zaufany E-bike - Gdańsk",
                   "Zaufany E-bike - Kraków",
                   "Zaufany E-bike - Poznań",
                   "Zaufany Rider - Warszawa",
                   "Zaufany Rider - Katowice",
                   "Zaufany Rider - Gdańsk",
                   "Zaufany Rider - Kraków",
                   "Zaufany Rider - Poznań"]:
        locations_position_id_dict[locations_position_id] = {
            "locations_position_id": locations_position_id,
            "vehicle": vehicle,
            "DS_name": clean_DS_name(DS_name)
        }

for key, value in locations_position_id_dict.items():
    match = re.match(pattern, value['vehicle'])
    if match:
        # Jeśli dopasowano do "e-Skuter Jush! - Zaufany Rider", zamień na "Zaufany Skuter"
        if match.group(1) == "e-Skuter Jush! - Zaufany Rider":
            value['vehicle'] = "Zaufany Skuter"
        else:
            value['vehicle'] = match.group(1)

df = pd.DataFrame.from_dict(locations_position_id_dict, orient="index")
df = df.merge(shiftplans_df, on='DS_name', how='left')
df = df.dropna(subset=['shiftplan_id'])
df['shiftplan_id'] = df['shiftplan_id'].astype('Int64')

print(df)


def main():
    # Wczytujemy pliki CSV
    df_shifts = pd.read_csv("shifts.csv")
    df_vehicles = pd.read_csv("vehicles.csv")

    # Modyfikacja df_shifts, żeby wszystko było w jednym wierszu
    df_shifts = df_shifts.melt(
        id_vars="place", var_name="shift_time", value_name="workers"
    )

    # Słownik do śledzenia dostępności pojazdów
    vehicle_availability = {}

    # Grupujemy pojazdy według lokalizacji i typu pojazdu
    grouped_vehicles = df_vehicles.groupby(["DS_name_v", "vehicle"]).size()

    for (place, vehicle), count in grouped_vehicles.items():  # type: ignore
        for shift_time in df_shifts["shift_time"].unique():
            # wynik: (Konstruktorska 13, 7-11, E-bike) = 1
            vehicle_availability[(place, shift_time, vehicle)] = count

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
        starts_at_iso = start_datetime.isoformat()
        ends_at_iso = end_datetime.isoformat()
        starts_at_iso = starts_at_iso[:16] + starts_at_iso[19:]
        ends_at_iso = ends_at_iso[:16] + ends_at_iso[19:]

        # Ustawienie kolejności. Ustawiamy zmienna vehicle na jedna z 3 elementow tej tablicy - to da nam gwarancje kolejnosci wrzucania zmian z priorytetem
        # poniewaz zmienna vehicle bedzie potem uzywana w kazdej operacji dotyczacej co wrzucamy i dlaczego wrzucamy :)
        for vehicle in ["Zaufany Skuter", "Zaufany E-bike", "Zaufany Rider"]:
            if (
                shift_time in ["10-15", "14-18", "18-22", "19-00:30"]
                and vehicle != "Zaufany Rider"
            ):
                continue

            # np, (Konstruktorska 13, 7-11, E-bike) // tak samo jak było w vehicle_availability :)
            vehicle_key = (place, shift_time, vehicle)
            #  Jeśli klucz istnieje w słowniku, funkcja zwróci przypisaną mu wartość (liczbę dostępnych pojazdów), a jeśli klucz nie istnieje, funkcja zwróci 0
            available_vehicle_count = vehicle_availability.get(vehicle_key, 0)

            # to kiedyś ogarnąć - logging
            if available_vehicle_count == 0 and vehicle != "Zaufany Rider":
                logging.info(
                    f"Brak dostępnych {vehicle} w {place} podczas {shift_time}"
                )
                continue

            # Pobieramy id lokalizacji i id zmiany dla naszych zmiennych z pliku positions.csv (matchujemy po miejscu i pojeździe i dla nich przypisujemy IDki)
            # np. index = 0,  locations_position_id = 174841,   vehicle = Zaufany E-bike,   DS_name = Szarych Szeregów 11,   shiftplan_id = 479246
            position_info = df[
                (df["DS_name"] == place)
                & (df["vehicle"] == vehicle)
            ]

            # Pomijamy, jeśli nie ma informacji o pozycji dla obecnego pojazdu
            if position_info.empty:
                logging.info(
                    f"Brak informacji o pozycji dla {vehicle} w {place}")
                continue

            # tutaj przypisujemy te IDki juz do pojedynczych zmiennych (wczesniej byly w dataframe)
            locations_position_id = position_info["locations_position_id"].values[0]
            shiftplan_id = position_info["shiftplan_id"].values[0]

            # tutaj przypisujemy do zmiennej workes_to_vehicle liczbę pojazdów jeśli takowa istnieje -
            # np. jeśli workers = 1 a avai._vehic._count = 5 to przypisze tylko 1 :) (ograniczenie wrzucania za duzej ilosci)
            # jeśli istnieje to odejmujemy ilość ze słownika (w sumie to jest nie potrzebne)
            if vehicle != "Zaufany Rider":
                workers_to_assign = min(workers, available_vehicle_count)
                vehicle_availability[vehicle_key] -= workers_to_assign
            else:
                # (a to dla sytuacji kiedy nie mamy zadnych pojazdow)
                workers_to_assign = workers

            # Przypisujemy pracownika do tego pojazdu i aktualizujemy dostępność
            api_call_post_shift(
                shiftplan_id=shiftplan_id,
                starts_at=starts_at_iso,
                ends_at=ends_at_iso,
                workers=workers_to_assign,
                locations_position_id=locations_position_id,
                note=vehicle,
            )

            # odejmujemy przypisanych pracowników od zmiennej workers, gdyby jeszcze bylo potrzebnych potrzeba przypisac kolejne
            # e-bikei lub zmiany zwykłe
            workers -= workers_to_assign
            # break w przypadku gdy juz nie mamy co przypisywać, przerywamy pętle dla tej iteracji
            if workers == 0:
                break


if __name__ == "__main__":
    main()
