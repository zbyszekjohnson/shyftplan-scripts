import os
import pandas as pd
import numpy as np
from pyairtable import Api
from logger import configure_logger
from settings import Settings


required_env_vars = ['ONFLEET_API_KEY_JUSH', 'AIRTABLE_API_KEY', 'AIRTABLE_APP_NAME',
                     'AIRTABLE_TABLE_NAME', 'AIRTABLE_VIEW_SHIFTS_UPDATING', 'AIRTABLE_VIEW_ALL_LEADS']


class AirtableAPI:

    def __init__(self, api_key, app_id, table_name):
        self.api = Api(api_key)
        self.table = self.api.table(app_id, table_name)

    def fetch_records(self, view):
        return self.table.all(view=view)

    def update_record(self, record_id, fields):
        return self.table.update(record_id, fields)

    def all_records(self):
        records = self.fetch_records(view="viwF00Er3j0ZePDMr")
        dict = {}
        for record in records:
            fields = record['fields']
            num_id, airtable_id = fields["ID"], fields["record_id"]
            dict[num_id] = {
                "airtable_id_long": airtable_id,
                "airtable_id": num_id
            }
        df = pd.DataFrame.from_dict(dict, orient='index')
        df.reset_index(drop=True, inplace=True)
        return df


class MainApp:
    def __init__(self):
        self.airtable = self.init_airtable()

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

    def id_to_check(self, airtable_records, leads):
        unique_ids_in_airtable = set(airtable_records['airtable_id'])
        unique_ids_in_leads = set(leads['airtable_id'])

        # Filtracja, aby pominąć NaN
        ids_only_in_airtable = {
            x for x in unique_ids_in_airtable if pd.notnull(x)}
        ids_only_in_leads = {x for x in unique_ids_in_leads if pd.notnull(x)}

        # Znajdowanie różnic w ID
        ids_only_in_airtable -= ids_only_in_leads
        ids_only_in_leads -= unique_ids_in_airtable

        print("ID do sprawdzenia", ids_only_in_leads)

    def cleaning_df(self, leads):
        leads_cleaned = leads.dropna(subset=['airtable_id'])
        print("Active Riders BINK: ", len(leads_cleaned))
        return leads_cleaned

    def merging_df(self, airtable_records, leads):
        result_df = pd.merge(airtable_records, leads,
                             on="airtable_id", how="inner")
        print("Riders Airtable Matched: ", len(result_df))
        return result_df

    def run(self):
        airtable_records = self.airtable.all_records()
        leads = pd.read_csv("leads.csv")
        leads.rename(columns={'Airtable ID': 'airtable_id'}, inplace=True)

        self.cleaning_df(leads)
        merged_df = self.merging_df(airtable_records, leads)
        print(merged_df)
        self.id_to_check(airtable_records, leads)

        for index, row in merged_df.iterrows():
            self.airtable.update_record(row["airtable_id_long"], {
                                        "Ile_zmian": row["Number of active Shifts"], "Ile_no_show": row["Number Of No Show"], "Active rider": True, })


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
