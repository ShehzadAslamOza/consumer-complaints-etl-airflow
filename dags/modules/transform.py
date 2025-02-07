import pandas as pd
import json
from datetime import datetime

from airflow.models.taskinstance import TaskInstance

COLUMNS_TO_KEEP = ['product','issue','sub_product','complaint_id','timely','company_response','submitted_via','company','date_received','state','sub_issue']

def transform(task_instance: TaskInstance):
    

    drilled_data = task_instance.xcom_pull(key="drilled_data", task_ids="load_sql")
    
    df = pd.DataFrame(drilled_data)
    
    # filtering columns
    df = df[COLUMNS_TO_KEEP]
    
    # transform date_received
    df['date_received'] = df['date_received'].apply(lambda timestamp: datetime.fromisoformat(timestamp).strftime('%d/%m/%Y'))
    
    df.rename(columns={'date_received':'Month Year'}, inplace=True)
    
    # drop complaint_id column
    df.drop(['complaint_id'], axis=1, inplace=True)

    # Group by all columns and count occurrences
    df_grouped = df.groupby(df.columns.tolist()).size().reset_index(name='Count of complaint_id')

    # sort by count of complaint_id
    df_grouped.sort_values(by=['Count of complaint_id'], ascending=False, inplace=True)

    transformed_data = df_grouped.to_json(orient='records')
    
    task_instance.xcom_push(key="transformed_data", value=transformed_data)

    print("Transformed Successfully")