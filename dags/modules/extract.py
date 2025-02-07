import requests
import json
from datetime import (date, timedelta)
from airflow.models.taskinstance import TaskInstance


URL = 'https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/?field=complaint_what_happened&size={}&date_received_max={}&date_received_min={}&state={}'
SIZE = 500
TIME_DELTA = 365
MAX_DATE = (date(2022,4,30)).strftime("%Y-%m-%d")
MIN_DATE = (date(2022,4,30) - timedelta(days=365)).strftime("%Y-%m-%d")
LIST_OF_STATES = ['AA', 'AE', 'AK', 'AL', 'AP', 'AR', 'AS', 'AZ', 'CA', 'CO', 'CT', 'DC', 'DE', 'FL', 'GA', 'GU', 'HI', 'IA', 'ID', 'IL', 'IN', 'KS', 'KY', 'LA', 'MA', 'MD', 'ME', 'MI', 'MN', 'MO', 'MP', 'MS', 'MT', 'NC', 'ND', 'NE', 'NH', 'NJ', 'NM', 'NV', 'NY', 'OH', 'OK', 'OR', 'PA', 'PR', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VA', 'VI', 'VT', 'WA', 'WI', 'WV', 'WY']


def extract(task_instance: TaskInstance):

    results = []
    count = 0
    
    for state in LIST_OF_STATES:
        try:
            response = requests.get(URL.format(SIZE, MAX_DATE, MIN_DATE, state)).json()
            results.extend(response['hits']['hits'])
            print(f"[{count}] {len(response['hits']['hits'])} results extracted for this state: {state}")
        except Exception as e:
            print(f"[-] Encountered an error extracting for this state: {state} \n \t\t response: {response} \n \t\t error: {e}")
            
        count += 1
        
    with open("./extracted_data.json","w") as f:
        json.dump(obj=results, fp=f, indent=4)
        
    # use xcom to push the results with the key 'extracted_data'
    task_instance.xcom_push(key="extracted_data", value=results)
        