import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy import DummyOperator
from pandas import DataFrame
import os
import pandas as pd
import psycopg2
import psycopg2.extras as extras


dag_name = 'ETL pipeline'
dag_id = 'migrate_data'


default_args = {
    'owner': 'airflow',    
    #'start_date': airflow.utils.dates.days_ago(2),
    # 'end_date': datetime(),
    # 'depends_on_past': False,
    #'email': ['airflow@example.com'],
    #'email_on_failure': False,
    # 'email_on_retry': False,
    # If a task fails, retry it once after waiting
    # at least 5 minutes
    'retries': 1
}

def run_etl_process():

    insert_query = """ 
        SELECT * FROM table_num;
    """

    # Connect to source postgres DB
    # source db_name : airflow

    conn_source_obj = psycopg2.connect(
    host="postgres",
    database="airflow",
    user="postgres",
    port="5432",
    password="postgres")

    # create cursor object
    cur = conn_source_obj.cursor() 
    print("source")
    cur.execute(insert_query)

    # load SQL data into Dataframe
    df = pd.DataFrame(cur.fetchall(), columns=['id', 'creation_date', 'sale_value'])
    print(df)

    # Connect to destination postgres DB
    # dest db_name : airflow_dest

    conn_dest_obj = psycopg2.connect(

        host="host.docker.internal", # postgres db with different host and port address
        database="airflow_dest",
        user="postgres",
        port="5433",
        password="postgres"
    )

    print("insert into destination")

    cur2 = conn_dest_obj.cursor() 

    tuples = [tuple(x) for x in df.to_numpy()]
    cols = ','.join(list(df.columns))

    create_table_query = """
        CREATE TABLE table_num (id INT NOT NULL, creation_date VARCHAR(250) NOT NULL, sale_value VARCHAR(250) NOT NULL);
    """

    cur2.execute(create_table_query)

    query = "INSERT INTO %s(%s) VALUES %%s" % ("table_num", cols) # insert to DB rows

    try:
        extras.execute_values(cur2, query, tuples)
        conn_dest_obj.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn_dest_obj.rollback()
        cur2.close()
        return 1
    print("--data migration complete")



with DAG(dag_id=dag_id,
         default_args=default_args,
         schedule_interval=None,
         max_active_runs=1,
         concurrency=1,
         start_date = airflow.utils.dates.days_ago(0)
         ) as dag:

    dummy_start = DummyOperator(task_id="start", dag=dag)

    etl_task = PythonOperator(task_id='source', python_callable=run_etl_process, dag=dag)
    
    # dest_task = PythonOperator(task_id='dest', python_callable=conn_source, dag=dag)

dummy_start >> etl_task




 
