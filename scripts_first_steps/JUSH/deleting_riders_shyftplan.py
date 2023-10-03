import requests
import csv
import os
user_email = os.environ['SHYFTPLAN_EMAIL']
authentication_token = os.environ['SHYFTPLAN_JUSH_API_KEY']

url = "https://shyftplan.com/api/v1/employments/"

params = {
    "user_email": user_email,
    "authentication_token": authentication_token,
    "company_id": 50272
}

with open('file.csv', 'r') as file:
    reader = csv.reader(file)
    for row in reader:
        employment_id = row[0]
        delete_url = url + employment_id
        print(delete_url)
        response = requests.delete(delete_url, params=params)
        print(response.text)
