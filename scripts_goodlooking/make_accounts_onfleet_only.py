import os
import pandas as pd
from pyairtable import Api
from onfleet import Onfleet
from logger import configure_logger
from settings import Settings


required_env_vars = ['ONFLEET_API_KEY_JUSH', 'AIRTABLE_API_KEY', 'AIRTABLE_APP_NAME',
                     'AIRTABLE_TABLE_NAME', 'AIRTABLE_VIEW_ACC_TO_CREATE',
                     'AIRTABLE_VIEW_REMOVED_RIDERS']


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
                    airtable_id, {"onfleet_rider_id": f"{of_id}"})
        else:
            script_logs += " //Numer już istnieje w Onfleet"
            airtable.update_record(
                airtable_id, {"script_logs": script_logs})
            return script_logs


class MainApp:
    def __init__(self):
        self.logger = configure_logger()
        self.airtable = self.init_airtable()
        self.onfleet = self.init_onfleet()

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

    def init_onfleet(self):
        return OnfleetAPI(Settings.ONFLEET_API_KEY_JUSH)

    def run(self):
        try:
            airtable_records = self.airtable.fetch_records(
                view=Settings.AIRTABLE_VIEW_ACC_BINK_TO_CREATE)
            if not airtable_records:
                self.logger.info("NO AIRTABLE RECORDS TO PROCESS...")
                exit(0)
            onfleet_workers_phones = self.onfleet.get_workers_phone_numbers()
            with open('message.txt', 'r') as file:
                self.message_content = file.read()

            for record in airtable_records:
                self.process_airtable_record(
                    record, onfleet_workers_phones)
        except Exception as e:
            self.logger.error(f"Problem before processing records: {e}")
            exit(1)

    def process_airtable_record(self, record, onfleet_workers_phones):
        try:
            df = self.airtable.deleted_riders_df()
            fields = record['fields']
            first_name, last_name, airtable_id = fields['Imię'], fields['Nazwisko'], fields['record_id']
            email, city, phone_number = fields['Adres e-mail'], fields['Miejscowość'], fields['Numer telefonu']
            script_logs, full_name = fields.get(
                'script_logs', ""), fields["Imię i nazwisko"]
            of_id = fields.get('onfleet_rider_id', "")
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
            return


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
