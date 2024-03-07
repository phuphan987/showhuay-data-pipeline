from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

import pandas as pd

def category_of_product(category_id):
    mysql_hook = MySqlHook(mysql_conn_id='mysql_showhuay')
    
    select_query = f'SELECT category_name FROM category WHERE category_id={category_id};'
    result = mysql_hook.get_records(select_query)[0][0]
    
    return result

def transform_users_data(**kwargs):
    mysql_data = kwargs['task_instance'].xcom_pull(task_ids='extract_task.extract_users_task')
    
    transformed_data = []
    for record in mysql_data:
        transformed_record = {
            'user_id': str(record[0]),
            'username': record[1],
            'email': record[2],
            'first_name': record[3],
            'last_name': record[4],
            'mobile_number': record[5],
        }
        transformed_data.append(transformed_record)
        
    df = pd.DataFrame(transformed_data)
    
    # replace nulls
    df['first_name'].fillna('buyer', inplace=True)
    df['last_name'].fillna('user', inplace=True)
    df['mobile_number'].fillna('N/A', inplace=True)
    
    transformed_data_filledna = df.to_dict(orient='records')
    
    return transformed_data_filledna

def transform_products_data(**kwargs):
    mysql_data = kwargs['task_instance'].xcom_pull(task_ids='extract_task.extract_products_task')
    
    transformed_data = []
    for record in mysql_data:
        transformed_record = {
            'product_id': record[0],
            'user_id': record[1],
            'category_name': category_of_product(record[2]),
            'product_name': record[3],
            'product_description': record[4],
            'price': record[5],
        }
        transformed_data.append(transformed_record)
        
    df = pd.DataFrame(transformed_data)
    
    df['user_id'].fillna('N/A', inplace=True)
    df['category_name'].fillna('N/A', inplace=True)
    df['product_name'].fillna('N/A', inplace=True)
    df['product_description'].fillna('N/A', inplace=True)
    df['price'].fillna('N/A', inplace=True)
    
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
def load_users_data(**kwargs):
    transformed_data_filledna = kwargs['task_instance'].xcom_pull(task_ids='transform_task.transform_users_task')

    postgres_hook = PostgresHook(postgres_conn_id='postgres_showhuay')

    insert_query = """
    INSERT INTO public.users (user_id, username, email, first_name, last_name, mobile_number)
    VALUES (%s, %s, %s, %s, %s, %s);
    """
    
    parameters = [(record['user_id'], record['username'], record['email'], record['first_name'], record['last_name'], record['mobile_number']) for record in transformed_data_filledna]
    for parameter in parameters:
        postgres_hook.run(insert_query, parameters=parameter)
        
#  CREATE TABLE IF NOT EXISTS products (
#    product_id VARCHAR(255),
#    user_id VARCHAR(255),
#    category_name VARCHAR(255),
#    product_name VARCHAR(255),
#    product_description text,
#    price double precision
#  );
def load_products_data(**kwargs):
    transform_data_filledna = kwargs['task_instance'].xcom_pull(task_ids='transform_task.transform_products_task')
    
    postgres_hook = PostgresHook(postgres_conn_id='postgres_showhuay')
    
    insert_query = """
    INSERT INTO public.products (product_id, user_id, category_name, product_name, product_description, price)
    VALUES (%s, %s, %s, %s, %s, %s);
    """
    
    parameters = [(record['product_id'], record['user_id'], record['category_name'], record['product_name'], record['product_description'], record['price']) for record in transform_data_filledna]
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
            sql='SELECT user_id, username, email, fname, lname, telephone_number FROM user',
            dag=dag,
        )
        
        extract_products_task = MySqlOperator(
            task_id='extract_products_task',
            mysql_conn_id='mysql_showhuay',
            sql='SELECT product_id, user_id, category_id, product_name, product_description, price FROM product',
            dag=dag,
        )
        
    with TaskGroup(group_id='transform_task') as transform_task:
    
        transform_users_task = PythonOperator(
            task_id='transform_users_task',
            python_callable=transform_users_data,
            dag=dag,
        )
        
        transform_products_task = PythonOperator(
            task_id='transform_products_task',
            python_callable=transform_products_data,
            dag=dag,
        )
        
    with TaskGroup(group_id='load_task') as load_task:
            
        load_users_task = PythonOperator(
            task_id='load_users_task',
            python_callable=load_users_data,
            dag=dag,
        )
        
        load_products_task = PythonOperator(
            task_id='load_products_task',
            python_callable=load_products_data,
            dag=dag
        )
    
    extract_task >> transform_task >> load_task