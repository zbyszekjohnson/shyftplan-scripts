import os
import requests
import pandas as pd
import time
from pyairtable import Api
from onfleet import Onfleet
from logger import configure_logger
from settings import Settings


required_env_vars = ['ONFLEET_API_KEY_JUSH', 'AIRTABLE_API_KEY', 'AIRTABLE_APP_NAME',
                     'AIRTABLE_TABLE_NAME', 'AIRTABLE_VIEW_ACC_TO_CREATE',
                     'AIRTABLE_VIEW_REMOVED_RIDERS', 'SHYFTPLAN_EMAIL', 'SHYFTPLAN_JUSH_API_KEY']


class ShyftplanAPI:

    BASE_URL = "https://shyftplan.com/api/v1/employments"
    BASE_URL_V2 = "https://shyftplan.com/api/v2/employments"
    BASE_URL_ADDING_USER = (f"https://shyftplan.com"
                            f"/api/v1/employments_positions")

    # we use it to filter exact locations_position_ids for
    # different places. It is the easiest and less problematic method
    POSITIONS_IDS = {
        "Warszawa": [180917, 180916, 144125, 141083],
        "Kraków": [180944, 180943, 146978, 146976],
        "Gdańsk": [180964, 180945, 151665, 150400],
        "Poznań": [151672, 149630, 149628],
        "Katowice": [150585, 150580]
    }

    def __init__(self, email, token):
        self.email = email
        self.token = token
        self.logger = configure_logger()

    def _construct_url(self, page, per_page):
        return (
            f"{self.BASE_URL}?user_email={self.email}"
            f"&authentication_token={self.token}"
            f"&page={page}&per_page={per_page}"
            f"&include_live_info=true&with_deleted=true&access_level=all"
            f"&order_key=last_name&order_dir=asc&with_deleted=true"
        )

    def fetch_accounts(self):
        MAX_ITERATIONS = 10
        accounts_list = []
        per_page = 1000
        page = 1
        headers = {"accept": "application/json"}

        response = requests.get(self._construct_url(
            page, per_page), headers=headers)

        accounts = response.json()

        total_items = accounts["total"]
        total_pages = (total_items + per_page - 1) // per_page

        accounts_list.extend(accounts["items"])

        for page in range(2, total_pages + 1):
            if page > MAX_ITERATIONS:
                self.logger.error("MAX_ITERATIONS exceeded")
                break
            response = requests.get(self._construct_url(
                page, per_page), headers=headers)
            accounts = response.json()
            accounts_list.extend(accounts["items"])

        return accounts_list

    def get_emails(self):
        accounts = self.fetch_accounts()
        return [account['email'] for account in accounts if account['email']]

    def create_shyftplan_account(self, first_name, last_name,
                                 email, phone_number):

        payload = {
            "sso_only": False,
            "maximum_money_enabled": False,
            "exit_month_payed_partially": False,
            "user_email": self.email,
            "authentication_token": self.token,
            "company_id": 50272,
            "first_name": first_name,
            "last_name": last_name,
            "email": email,
            "phone_number": "+48" + phone_number,
            "password": "jush1234"
        }
        headers = {
            "accept": "application/json",
            "content-type": "application/x-www-form-urlencoded"
        }

        response = requests.post(
            self.BASE_URL_V2, data=payload, headers=headers)
        return response

    def adding_user_to_location_function(self,
                                         shyftplan_user_id, locations_position_id):

        payload = {
            "user_email": self.email,
            "authentication_token": self.token,
            "employment_id": shyftplan_user_id,
            "locations_position_id": locations_position_id
        }
        headers = {
            "accept": "application/json",
            "content-type": "application/json"
        }

        response = requests.post(
            self.BASE_URL_ADDING_USER, json=payload, headers=headers)
        # to do not overload API
        time.sleep(0.5)
        return response

    def get_all_locations_position_id(self):

        url = (f"https://shyftplan.com/api/v1/locations_positions"
               f"?user_email={self.email}&authentication_token={self.token}"
               f"&page=1&per_page=1000&")

        headers = {"accept": "application/json"}
        response = requests.get(url, headers=headers)
        data = response.json()
        return data.get("items", [])

    def make_dict_from_data(self, raw_data):
        dictionary = {}
        for item in raw_data:
            locations_position_id = item['id']
            position_id = item['position_id']
            dictionary[locations_position_id] = {
                "locations_position_id": locations_position_id,
                "position_id": position_id
            }
        return dictionary

    def handle_shyftplan_error(self, email, api_call_status_code, record_id, airtable, script_logs):
        self.logger.error(f"Błąd przy tworzeniu konta w Shyftplan dla {email}. "
                          f"Kod statusu: {api_call_status_code}")
        script_logs += f" // Shyftplan problem: Kod: {api_call_status_code}"
        airtable.update_record(record_id, {"script_logs": script_logs})
        return script_logs

    def return_id(self, api_call):
        api_call_data = api_call.json()
        shyftplan_id = api_call_data.get('id', None)
        return shyftplan_id

    def return_user_id(self, api_call):
        api_call_data = api_call.json()
        shyftplan_user_id = api_call_data.get('user_id', None)
        return shyftplan_user_id

    def return_user_id_v2(self, email, shyftplan_data):
        shyftplan_users_emails = shyftplan_data
        user_id_row = shyftplan_users_emails[shyftplan_users_emails['email'] == email]
        if not user_id_row.empty:
            user_id = user_id_row.index[0]
        else:
            user_id = None
        print("USER_ID: ", user_id, "EMAIL: ", email)
        return int(user_id)

    def return_user_id_v3(self, email, shyftplan_data_v2):
        shyftplan_users_emails = shyftplan_data_v2
        user_id_row = shyftplan_users_emails[shyftplan_users_emails['email'] == email]
        if not user_id_row.empty:
            id = user_id_row.index[0]
        else:
            id = None
        print("ID: ", id, "EMAIL: ", email)
        return int(id)

    def adding_user_to_locations_process(self, city, loc_pos_df, shyftplan, shyftplan_user_id):
        position_ids_for_city = ShyftplanAPI.POSITIONS_IDS[city]

        for pos_id in position_ids_for_city:
            matching_locations = loc_pos_df[loc_pos_df['position_id'] ==
                                            pos_id]['locations_position_id'].tolist()

            for loc_id in matching_locations:
                shyftplan.adding_user_to_location_function(
                    shyftplan_user_id, loc_id)

    def get_data_users(self):
        accounts = self.fetch_accounts()
        email_dict = {}
        for item in accounts:
            user_id = item["id"]
            user_email = item["email"]
            email_dict[user_id] = {
                "email": user_email
            }
        df = pd.DataFrame.from_dict(email_dict, orient="index")
        return df

    def get_data_users_v2(self):
        accounts = self.fetch_accounts()
        email_dict = {}
        for item in accounts:
            user_id = item["user_id"]
            user_email = item["email"]
            email_dict[user_id] = {
                "email": user_email
            }
        df = pd.DataFrame.from_dict(email_dict, orient="index")
        return df

    def restore_user(self, user_id):

        url = f"https://shyftplan.com/api/v1/employments/{user_id}/restore_employment"

        payload = {
            "user_email": Settings.SHYFTPLAN_EMAIL,
            "authentication_token": Settings.SHYFTPLAN_JUSH_API_KEY,
            "company_id": 50272
        }
        headers = {
            "accept": "application/json",
            "content-type": "application/json"
        }

        response = requests.post(url, json=payload, headers=headers)
        if response.status_code == 201:
            print("User restored")


class AirtableAPI:

    def __init__(self, api_key, app_id, table_name):
        self.api = Api(api_key)
        self.table = self.api.table(app_id, table_name)

    def fetch_records(self, view):
        return self.table.all(view=view)

    def update_record(self, record_id, fields):
        return self.table.update(record_id, fields)

    def deleted_riders_df(self):
        records = self.fetch_records(
            view=Settings.AIRTABLE_VIEW_REMOVED_RIDERS)
        dict = {}
        for record in records:
            fields = record['fields']
            full_name, phone_number, email = fields["Imię i nazwisko"], fields[
                "Numer telefonu"], fields['Adres e-mail']
            full_name = ' '.join(full_name.split())
            dict[full_name] = {
                "full_name": full_name,
                "phone_number": phone_number,
                "email": email
            }
        df = pd.DataFrame.from_dict(dict, orient='index')
        df.reset_index(drop=True, inplace=True)
        return df

    def possible_bad_guy(self, airtable_id, script_logs):
        script_logs += " //POSSIBLE BAD GUY"
        self.update_record(
            airtable_id, {"script_logs": script_logs})
        return script_logs

    def report_problem(self, airtable_id, script_logs, problem_content):
        script_logs += f" {problem_content}"
        self.update_record(
            airtable_id, {"script_logs": script_logs})
        return script_logs


class OnfleetAPI:
    def __init__(self, key):
        self.api = Onfleet(api_key=key)

    def get_all_workers(self):
        return self.api.workers.get()

    def create_worker_from_record(self, first_name, last_name, phone_number):
        data = {
            "name": f"{first_name} {last_name}",
            "phone": f"+48{phone_number}",
            "teams": ["<2r3CLTAJmDxR8eO*QRSe1wmj>"],
            "vehicle": {
                "type": "MOTORCYCLE"
            }
        }
        response = self.api.workers.create(body=data)
        return response['id']

    def get_teams_id(self):
        return self.api.teams.get()

    def get_workers_phone_numbers(self):
        workers = self.get_all_workers()
        return [worker['phone'] for worker in workers if worker['phone']]

    def process_record_onfleet(self, airtable, airtable_id, phone_number, onfleet_workers_phones, first_name, last_name, script_logs, of_id):
        if "+48" + phone_number not in onfleet_workers_phones:
            onfleet_user_id = self.create_worker_from_record(
                first_name, last_name, phone_number)
            airtable.update_record(
                airtable_id, {"Onfleet - wpisane": True})

            if not of_id:
                airtable.update_record(
                    airtable_id, {"onfleet_rider_id": f"{onfleet_user_id}"})
            else:
                airtable.update_record(
                    airtable_id, {"onfleet_rider_id": f"{of_id};xDDDDD"})
        else:
            script_logs += " //Numer już istnieje w Onfleet"
            airtable.update_record(
                airtable_id, {"script_logs": script_logs})
            return script_logs


class MainApp:
    def __init__(self):
        self.logger = configure_logger()
        self.check_env_vars()
        self.airtable = self.init_airtable()
        self.shyftplan = self.init_shyftplan()
        self.onfleet = self.init_onfleet()

    def check_env_vars(self):
        missing_vars = []
        for var in required_env_vars:
            if var not in os.environ:
                missing_vars.append(var)
        if missing_vars:
            self.logger.error(
                f"Brakujące zmienne środowiskowe: {', '.join(missing_vars)}")
            exit(1)

    def init_airtable(self):
        return AirtableAPI(
            Settings.AIRTABLE_API_KEY, Settings.AIRTABLE_APP_NAME,
            Settings.AIRTABLE_TABLE_NAME
        )

    def init_shyftplan(self):
        return ShyftplanAPI(
            Settings.SHYFTPLAN_EMAIL, Settings.SHYFTPLAN_JUSH_API_KEY
        )

    def init_onfleet(self):
        return OnfleetAPI(Settings.ONFLEET_API_KEY_JUSH)

    def run(self):
        try:
            airtable_records = self.airtable.fetch_records(
                view=Settings.AIRTABLE_VIEW_ACC_TO_CREATE)
            if not airtable_records:
                self.logger.info("NO AIRTABLE RECORDS TO PROCESS...")
                exit(0)
            onfleet_workers_phones = self.onfleet.get_workers_phone_numbers()
            shyftplan_emails = self.shyftplan.get_emails()
            shyftplan_data = self.shyftplan.get_data_users()
            shyftplan_data_v2 = self.shyftplan.get_data_users_v2()
            locations_position_id_dict_raw = self.shyftplan.get_all_locations_position_id()
            locations_position_id_dict = self.shyftplan.make_dict_from_data(
                locations_position_id_dict_raw)
            loc_pos_df = pd.DataFrame.from_dict(
                locations_position_id_dict, orient="index")
            loc_pos_df.reset_index(drop=True, inplace=True)
            for record in airtable_records:
                self.process_airtable_record(
                    record, shyftplan_emails, loc_pos_df, onfleet_workers_phones, shyftplan_data, shyftplan_data_v2)
        except Exception as e:
            self.logger.error(f"Problem before processing records: {e}")
            exit(1)

    def process_airtable_record(self, record, shyftplan_emails, loc_pos_df, onfleet_workers_phones, shyftplan_data, shyftplan_data_v2):
        try:
            df = self.airtable.deleted_riders_df()
            fields = record['fields']
            first_name, last_name, airtable_id = fields['Imię'], fields['Nazwisko'], fields['record_id']
            email, city, phone_number = fields['Adres e-mail'], fields['Miejscowość'], fields['Numer telefonu']
            script_logs, full_name = fields.get(
                'script_logs', ""), fields["Imię i nazwisko"]
            sh_id, of_id = fields.get('shyftplan_user_id', ""), fields.get(
                'onfleet_rider_id', "")
            if phone_number.startswith("+48"):
                phone_number = phone_number[3:]
            elif phone_number.startswith("48"):
                phone_number = phone_number[2:]

            # FILTER for deleted riders
            if email in df['email'].values:
                self.airtable.possible_bad_guy(airtable_id, script_logs)
                return
            if phone_number in df['phone_number'].values:
                self.airtable.possible_bad_guy(airtable_id, script_logs)
                return
            if full_name in df['full_name'].values:
                self.airtable.possible_bad_guy(airtable_id, script_logs)
                return

            # START of actions to process the record
            if email not in shyftplan_emails:
                api_call = self.shyftplan.create_shyftplan_account(
                    first_name, last_name, email, phone_number)
                shyftplan_user_id = self.shyftplan.return_id(api_call)
                sh_id_airtable = self.shyftplan.return_user_id(
                    api_call)

                if api_call.status_code == 201:
                    if not sh_id:
                        self.airtable.update_record(
                            airtable_id, {"shyftplan_user_id": f"{sh_id_airtable}"})
                    else:
                        self.airtable.update_record(
                            airtable_id, {"shyftplan_user_id": f"{sh_id};{sh_id_airtable}"})

                if api_call.status_code != 201:
                    script_logs = self.shyftplan.handle_shyftplan_error(
                        email, api_call.status_code, airtable_id, self.airtable, script_logs)
                    return

                self.airtable.update_record(
                    airtable_id, {"Shyftplan - wpisane": True})

            # Adding this mail to list in case of duplicates
                shyftplan_emails.append(email)
            # Adding to postions in Shyftplan
                if city in ShyftplanAPI.POSITIONS_IDS:
                    self.shyftplan.adding_user_to_locations_process(
                        city, loc_pos_df, self.shyftplan, shyftplan_user_id)

            # Case when record had shyftplan before
            else:
                shyftplan_user_id = self.shyftplan.return_user_id_v2(
                    email, shyftplan_data)
                sh_id_airtable = self.shyftplan.return_user_id_v3(
                    email, shyftplan_data_v2)
                self.shyftplan.restore_user(shyftplan_user_id)
                self.airtable.update_record(
                    airtable_id, {"Shyftplan - wpisane": True})
                self.shyftplan.adding_user_to_locations_process(
                    city, loc_pos_df, self.shyftplan, shyftplan_user_id)
                if not sh_id:
                    self.airtable.update_record(
                        airtable_id, {"shyftplan_user_id": f"{sh_id_airtable}"})
                else:
                    self.airtable.update_record(
                        airtable_id, {"shyftplan_user_id": f"{sh_id};{sh_id_airtable}"})

            try:
                script_logs = self.onfleet.process_record_onfleet(
                    self.airtable, airtable_id, phone_number,
                    onfleet_workers_phones, first_name, last_name, script_logs, of_id)
            except Exception:
                script_logs = self.airtable.report_problem(
                    self, airtable_id, script_logs, "ONFLEET PROBLEM")

        except Exception as e:
            self.logger.error(
                e, f"Problem przy przetwarzaniu rekordu {airtable_id}")
            script_logs = self.airtable.report_problem(
                self, airtable_id, script_logs, "OTHER PROBLEM")
            exit(1)


def main():
    try:
        app = MainApp()
        app.run()

    except Exception as inner_e:
        logger = configure_logger()
        logger.error(f"Problem with app.run: {inner_e}")
        exit(1)


if __name__ == "__main__":
    result = main()
    exit(result)
