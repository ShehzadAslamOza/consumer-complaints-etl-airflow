import pymysql
import json
from airflow.models.taskinstance import TaskInstance

def load_sql(task_instance: TaskInstance):

    extracted_data = task_instance.xcom_pull(key="extracted_data", task_ids="extract")
            
    drilled_data = []

    for result in extracted_data:
        drilled_data.append(result['_source'])


    create_complaint_table = """
    CREATE TABLE IF NOT EXISTS complaints (
        complaint_id INT PRIMARY KEY,
        product VARCHAR(255),
        complaint_what_happened TEXT,
        date_sent_to_company DATETIME,
        issue VARCHAR(255),
        sub_product VARCHAR(255),
        zip_code VARCHAR(10),
        tags TEXT,
        has_narrative BOOLEAN,
        timely VARCHAR(10),
        consumer_consent_provided VARCHAR(50),
        company_response VARCHAR(50),
        submitted_via VARCHAR(50),
        company VARCHAR(255),
        date_received DATETIME,
        state VARCHAR(2),
        consumer_disputed VARCHAR(10),
        company_public_response TEXT,
        sub_issue VARCHAR(255)
    );
    """

    records = []

    for data in drilled_data:
        records.append(( data["complaint_id"],
        data["product"],
        data["complaint_what_happened"],
        data["date_sent_to_company"],
        data["issue"],
        data["sub_product"],
        data["zip_code"],
        data["tags"],
        data["has_narrative"],
        data["timely"],
        data["consumer_consent_provided"],
        data["company_response"],
        data["submitted_via"],
        data["company"],
        data["date_received"],
        data["state"],
        data["consumer_disputed"],
        data["company_public_response"],
        data["sub_issue"]))
        
        
    try:
        connection = pymysql.connect(host='',
                        port = 3306,
                        user='',
                        password='',
                        database='',
                        cursorclass=pymysql.cursors.DictCursor)



        with connection:    
            with connection.cursor() as cursor:
                
                # Create table
                sql = create_complaint_table
                cursor.execute(sql)
                print(f'The query affected {cursor.rowcount} rows')

                rows = cursor.fetchall()
                print(f'{rows}')           
                
                # Clear the table
                sql = "DELETE FROM complaints"
                cursor.execute(sql)
                print(f'The query affected {cursor.rowcount} rows')

                rows = cursor.fetchall()
                print(f'{rows}')       
                
                # Add Data to table
                query = """
                    INSERT INTO complaints (
                        complaint_id, product, complaint_what_happened, date_sent_to_company, 
                        issue, sub_product, zip_code, tags, has_narrative, timely, 
                        consumer_consent_provided, company_response, submitted_via, company, 
                        date_received, state, consumer_disputed, company_public_response, sub_issue
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                cursor.executemany(query, records)
                print(f'The query affected {cursor.rowcount} rows')
            # connection is not autocommit by default. So you must commit to save
            # your changes.
            connection.commit()
    except Exception as e:
        print(f"[-] Error: Unexpected error: Could not dump data to MYSQL. \n\t {e}")
        print(e)
        
        print(f"[-] Saving data to JSON file")
        # Save the drill data
        with open('data_to_sql_fallback.json', 'w') as outfile:
            json.dump(drilled_data, outfile)

    # Sending the drilled data to transform stage to save on computation
    task_instance.xcom_push(key="drilled_data", value=drilled_data)