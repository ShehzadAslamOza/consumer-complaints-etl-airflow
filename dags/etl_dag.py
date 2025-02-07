from airflow.decorators import dag
from datetime import datetime

from airflow.operators.python import PythonOperator
from modules import (extract, load_sql, transform, load_gsheet)

@dag(schedule_interval=None, start_date=datetime(2023, 8, 1), catchup=True)
def my_simple_dag():

    extract_data = PythonOperator(
        task_id="extract",
        python_callable=extract.extract
    )

    load_to_sql = PythonOperator(
        task_id="load_sql",
        python_callable=load_sql.load_sql
    )
    
    transform_data = PythonOperator(
        task_id="transform",
        python_callable=transform.transform
    )

    load_to_gsheet = PythonOperator(
        task_id="load_gsheet",
        python_callable=load_gsheet.load_gsheet
    )

    extract_data >> load_to_sql >> transform_data >> load_to_gsheet
     

simple_dag = my_simple_dag()
