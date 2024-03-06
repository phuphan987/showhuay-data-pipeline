from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

import pandas as pd

def transform_data(**kwargs):
    mysql_data = kwargs['task_instance'].xcom_pull(task_ids='extract_task')
    print(f'kwargs = {kwargs}')
    print(f'kwargs[ti] = {kwargs["ti"]}')
    print(f'kwargs[task_instance] = {kwargs["task_instance"]}')
    
    transformed_data = []
    for record in mysql_data:
        transformed_record = {
            'user_id': str(record[0]),
            'username': record[1],
            'email': record[3],
            'first_name': record[4],
            'last_name': record[5],
            'mobile_number': record[6],
        }
        transformed_data.append(transformed_record)
        
    df = pd.DataFrame(transformed_data)
    
    # replace nulls
    df['first_name'].fillna('buyer', inplace=True)
    df['last_name'].fillna('user', inplace=True)
    df['mobile_number'].fillna('N/A', inplace=True)
    
    transformed_data_filledna = df.to_dict(orient='records')
    
    return transformed_data_filledna

#  CREATE TABLE IF NOT EXISTS users (
#    user_id VARCHAR(255),
#    username VARCHAR(255),
#    email VARCHAR(255),
#    first_name VARCHAR(255),
#    last_name VARCHAR(255),
#    mobile_number VARCHAR(255)
#  );
def load_data(**kwargs):
    transformed_data_filledna = kwargs['task_instance'].xcom_pull(task_ids='transform_task')

    postgres_hook = PostgresHook(postgres_conn_id='postgres_showhuay')
    
    insert_query = """
    INSERT INTO public.users (user_id, username, email, first_name, last_name, mobile_number)
    VALUES (%s, %s, %s, %s, %s, %s);
    """
    
    parameters = [(record['user_id'], record['username'], record['email'], record['first_name'], record['last_name'], record['mobile_number']) for record in transformed_data_filledna]
    for parameter in parameters:
        postgres_hook.run(insert_query, parameters=parameter)

default_args = {
    'owner': 'phuphan987',
    'start_date': datetime(2024, 3, 1),
    'email': ['airflow@admin.com'],
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='showhuay_data_pipeline',
    schedule='@daily',
    default_args=default_args,
    description='A simple data pipeline for showhuay e-commerce website',
    catchup=False
) as dag:
    
    with TaskGroup(group_id='extract_task') as extract_task:
        
        extract_users_task = MySqlOperator(
            task_id='extract_users_task',
            mysql_conn_id='mysql_showhuay',
            sql='SELECT * FROM user',
            dag=dag,
        )
        
        extract_products_task = MySqlOperator(
            task_id='extract_products_task',
            mysql_conn_id='mysql_showhuay',
            sql='SELECT * FROM product',
            dag=dag,
        )
    
    transform_task = PythonOperator(
        task_id='transform_task',
        python_callable=transform_data,
        dag=dag,
    )
    
    load_task = PythonOperator(
        task_id='load_task',
        python_callable=load_data,
        dag=dag,
    )
    
    extract_task >> transform_task >> load_task