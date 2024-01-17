import os
import requests
import datetime
import pandas as pd
import time
from pyairtable import Api
from onfleet import Onfleet
from logger import configure_logger
from settings import Settings
import pprint


required_env_vars = ['ONFLEET_API_KEY_JUSH', 'AIRTABLE_API_KEY', 'AIRTABLE_APP_NAME',
                     'AIRTABLE_TABLE_NAME', 'AIRTABLE_VIEW_SHIFTS_UPDATING', 'AIRTABLE_VIEW_ALL_LEADS',
                     'SHYFTPLAN_EMAIL', 'SHYFTPLAN_JUSH_API_KEY']


class AirtableAPI:

    def __init__(self, api_key, app_id, table_name):
        self.api = Api(api_key)
        self.table = self.api.table(app_id, table_name)

    def fetch_records(self, view):
        return self.table.all(view=view)

    def update_record(self, record_id, fields):
        return self.table.update(record_id, fields)

    def make_df_from_records(self):
        airtable_records = self.fetch_records(
            view=Settings.AIRTABLE_VIEW_ALL_LEADS)
        dict_airtable = {}
        for record in airtable_records:
            fields = record['fields']
            full_name = fields.get("Imię i nazwisko", None)
            sh_user_id = fields.get("shyftplan_user_id", None)
            airtable_id = fields.get("record_id", None)
            dict_airtable[full_name] = {
                "sh_user_id": sh_user_id,
                "airtable_id": airtable_id
            }
        df = pd.DataFrame.from_dict(dict_airtable, orient='index')
        df['sh_user_id'] = df['sh_user_id'].str.split(';')
        df = df.explode('sh_user_id')
        df.reset_index(drop=True, inplace=True)
        return df


class ShyftplanAPI:

    def __init__(self, email, token):
        self.email = email
        self.token = token
        self.logger = configure_logger()

    def _construct_url(self, page, per_page, updated_after, state=None):
        base_url = (
            f"https://shyftplan.com/api/v1/evaluations?user_email={self.email}&"
            f"authentication_token={self.token}&company_id=50272&page={page}&per_page={per_page}&"
            f"include_payments=false&updated_after={updated_after}"
        )
        if state:
            base_url += f"&state={state}"

        return base_url

    def list_all_evaluations(self, state=None):
        MAX_ITERATIONS = 60
        today = datetime.date.today()
        print(today)
        date_30_days_ago = today - datetime.timedelta(days=60)
        updated_after = date_30_days_ago.strftime('%Y-%m-%d')
        print(updated_after)

        evaulation_list = []
        per_page = 500
        page = 1
        headers = {"accept": "application/json"}
        response = requests.get(self._construct_url(
            page, per_page, updated_after, state), headers=headers)

        evaulations = response.json()

        total_items = evaulations["total"]
        total_pages = (total_items + per_page - 1) // per_page
        print('total_pages: ', total_pages)

        evaulation_list.extend(evaulations["items"])

        for page in range(2, total_pages + 1):
            if page > MAX_ITERATIONS:
                self.logger.error("MAX_ITERATIONS exceeded")
                break
            response = requests.get(self._construct_url(
                page, per_page, updated_after, state), headers=headers)
            evaulations = response.json()
            evaulation_list.extend(evaulations["items"])
        return evaulation_list

    def employment_list(self):
        url = (f"https://shyftplan.com/api/v1/employments?user_email={self.email}"
               f"&authentication_token={self.token}&company_id=50272&"
               f"page=1&per_page=1000&include_live_info=true&"
               f"with_deleted=true&access_level=all&order_key=last_name&order_dir=asc")
        headers = {"accept": "application/json"}
        response = requests.get(url, headers=headers)
        data = response.json()
        return data.get("items", [])

    def sp_list_both_ids(self):
        raw_data = self.employment_list()
        dict = {}
        for item in raw_data:
            id = item['id']
            user_id = item['user_id']
            dict[id] = {
                "user_id": user_id,
                "id": id
            }
        df = pd.DataFrame.from_dict(dict, orient="index")
        df = df.reset_index(drop=True)
        return df


class MainApp:
    def __init__(self):
        self.logger = configure_logger()
        self.shyftplan = self.init_shyftplan()
        self.airtable = self.init_airtable()
        self.check_env_vars()

    def check_env_vars(self):
        missing_vars = []
        for var in required_env_vars:
            if var not in os.environ:
                missing_vars.append(var)
        if missing_vars:
            self.logger.error(
                f"Brakujące zmienne środowiskowe: {', '.join(missing_vars)}")
            exit(1)

    def init_shyftplan(self):
        return ShyftplanAPI(
            Settings.SHYFTPLAN_EMAIL, Settings.SHYFTPLAN_JUSH_API_KEY
        )

    def init_airtable(self):
        return AirtableAPI(
            Settings.AIRTABLE_API_KEY, Settings.AIRTABLE_APP_NAME,
            Settings.AIRTABLE_TABLE_NAME
        )

    def get_df_to_match(self):
        df_airtable = self.airtable.make_df_from_records()
        df_shyftplan = self.shyftplan.sp_list_both_ids()
        df_airtable['sh_user_id'] = df_airtable['sh_user_id'].astype(str)
        df_shyftplan['user_id'] = df_shyftplan['user_id'].astype(str)
        merged_df = df_airtable.merge(
            df_shyftplan, how='left', left_on='sh_user_id', right_on='user_id')
        merged_df.drop(columns=['sh_user_id'], inplace=True)

        merged_df['id'] = merged_df['id'].fillna(0).astype(int)
        merged_df['user_id'] = merged_df['user_id'].fillna(0).astype(int)
        merged_df = merged_df[merged_df['user_id'] != 0]
        return merged_df

    def get_records(self, state=None):
        df_to_match = self.get_df_to_match()
        df_evaluation_list = pd.DataFrame(
            self.shyftplan.list_all_evaluations(state=state))

        if state == "no_show":
            column_name = 'no_show_count'
        else:
            column_name = 'shift_Count'

        evaluation_counts = df_evaluation_list.groupby('user_id').size()
        evaluation_counts.name = column_name
        df_counts = evaluation_counts.reset_index()

        df_merged = df_to_match.merge(df_counts, on='user_id', how='left')
        df_filtered = df_merged.dropna(subset=[column_name])

        return df_filtered

    def run(self):
        x = self.shyftplan.list_all_evaluations()
        filtered_evaluations = [e for e in x if e['state'] != 'no_show']
        pprint.pprint(filtered_evaluations)

        # Przekształca datę i grupuje po user_id, zachowując najpóźniejszą datę
        latest_dates = {}

        for evaluation in filtered_evaluations:
            user_id = evaluation['user_id']
            date = datetime.datetime.strptime(
                evaluation['evaluation_starts_at'], '%Y-%m-%dT%H:%M:%S.%f%z').date()

            if user_id not in latest_dates or latest_dates[user_id] < date:
                latest_dates[user_id] = date

        # Wynik
        result = [{'user_id': user_id, 'latest_date': str(
            latest_date)} for user_id, latest_date in latest_dates.items()]

        # # Wydrukuj wynik
        # for item in result:
        #     print(item)

        pprint.pprint(x)
        records = self.get_records()
        records_with_no_show = self.get_records(state="no_show")
        print(records)
        print(records_with_no_show)
        for index, row in records.iterrows():
            self.airtable.update_record(
                row['airtable_id'], {"Ile_zmian": (row['shift_Count'])})
            self.airtable.update_record(
                row['airtable_id'], {"Active rider": True})
            time.sleep(0.5)

        for index, row in records_with_no_show.iterrows():
            self.airtable.update_record(
                row['airtable_id'], {"Ile_no_show": (row['no_show_count'])})
            time.sleep(0.5)


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
