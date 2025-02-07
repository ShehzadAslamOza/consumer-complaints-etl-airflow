import gspread
import json
import pandas as pd
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials

credentials_dict = {
  "type": "",
  "project_id": "",
  "private_key_id": "",
  "private_key": "",
  "client_email": "",
  "client_id": "",
  "auth_uri": "",
  "token_uri": "",
  "auth_provider_x509_cert_url": "",
  "client_x509_cert_url": "",
  "universe_domain": ""
}


GSHEET_URL = ''
GHSEET_ID = ''

from airflow.models.taskinstance import TaskInstance

def load_gsheet(task_instance: TaskInstance):

    
    transformed_data = task_instance.xcom_pull(key="transformed_data", task_ids="transform")
    transformed_data = json.loads(transformed_data)
    df = pd.DataFrame(transformed_data)
    transformed_data_csv = df.to_csv()
    
    
    scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
         "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]

    credentials = ServiceAccountCredentials.from_json_keyfile_dict(credentials_dict, scope)
    gc = gspread.authorize(credentials)
    sht1 = gc.open_by_key(GHSEET_ID)
    worksheet = sht1.get_worksheet(0)
    worksheet.clear()
    gc.import_csv(GHSEET_ID, data=transformed_data_csv)
    modify_text = "Modified on " + datetime.now().strftime("%H:%M:%S %d/%m/%Y") + " by Airflow"
    worksheet.update_cell(1, 1, modify_text )
    print("Added to Google Sheet Successfully")