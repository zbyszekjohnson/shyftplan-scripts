import csv
import os
from onfleet import Onfleet

api = Onfleet(api_key=os.environ['ONFLEET_API_KEY_DELIO'])

with open('file.csv', 'r') as file:
    reader = csv.reader(file)
    for row in reader:
        worker_id = row[0]
        try:
            api.workers.deleteOne(worker_id)
            print(f"Successfully deleted worker with ID: {worker_id}")
        except Exception as e:
            print(f"Failed to delete worker with ID: {worker_id}. Error: {e}")
