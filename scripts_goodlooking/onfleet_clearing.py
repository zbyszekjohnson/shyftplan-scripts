import os
import pandas as pd
from pyairtable import Api
from onfleet import Onfleet
from logger import configure_logger
from settings import Settings
import serwersms
import pprint


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


class OnfleetAPI:
    def __init__(self, key):
        self.api = Onfleet(api_key=key)

    def delete_worker(self, of_id):
        return self.api.workers.deleteOne(id=f"{of_id}")

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


class MainApp:
    def __init__(self):
        self.logger = configure_logger()
        self.check_env_vars()
        self.airtable = self.init_airtable()
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

    def confirmation_sms(self, phone_number):
        api = serwersms.SerwerSMS(Settings.SERWER_SMS_API)
        try:
            params = {
                'details': 'true'
            }
            response = api.message.send_sms(
                phone_number, self.message_content, 'JUSH', params)
        except Exception:
            self.logger.error(f"Problem at sending sms")

    def init_airtable(self):
        return AirtableAPI(
            Settings.AIRTABLE_API_KEY, Settings.AIRTABLE_APP_NAME,
            Settings.AIRTABLE_TABLE_NAME
        )

    def init_onfleet(self):
        return OnfleetAPI("Settings.ONFLEET_API_KEY_JUSH")

    def join_ids(self, file_path):
        with open(file_path, 'r') as file:
            ids = file.read().splitlines()
        return ';'.join(ids)

    def process_airtable_record(self, record):
        fields = record['fields']
        # of_ids = fields.get('onfleet_rider_id', "")
        # for of_id in of_ids.split(';'):
        #     try:
        #         # przetwarzanie każdego ID osobno
        #         x = self.onfleet.delete_worker(of_id.strip())
        #     except Exception as e:
        #         self.logger.error(
        #             f"Problem przy przetwarzaniu ID {of_id}: {e}")

    def run(self):
        try:
            airtable_records = self.airtable.fetch_records(
                view="viwq7D81pYO09WfpC")
            d = 'tekst.csv'
            x = self.join_ids(d)
            print(x)

            if not airtable_records:
                self.logger.info("NO AIRTABLE RECORDS TO PROCESS...")
                exit(0)

            for record in airtable_records:
                self.process_airtable_record(record)
        except Exception as e:
            self.logger.error(f"Problem before processing records: {e}")
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
