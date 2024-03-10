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
            'user_id': record[0],
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

def transform_orders_data(**kwargs):
    mysql_data = kwargs['task_instance'].xcom_pull(task_ids='extract_task.extract_orders_task')
    
    transformed_data = []
    for record in mysql_data:
        transformed_record = {
            'order_id': record[0],
            'user_id': record[1],
            'product_id': record[2],
            'product_name': record[3],
            'price': record[4],
            'quantity': record[5],
            'total_price': record[6],
            'purchase_timestamp': record[7],
            'payment_timestamp': record[8],
        }
        transformed_data.append(transformed_record)
        
    df = pd.DataFrame(transformed_data)
    
    dt_string = datetime.now().strftime('%d-%m-%Y %H:%i:%s')
    
    df['user_id'].fillna('N/A', inplace=True)
    df['product_id'].fillna('N/A', inplace=True)
    df['product_name'].fillna('N/A', inplace=True)
    df['price'].fillna(0, inplace=True)
    df['quantity'].fillna(0, inplace=True)
    df['total_price'].fillna(0, inplace=True)
    df['purchase_timestamp'].fillna(dt_string, inplace=True)
    df['payment_timestamp'].fillna(dt_string, inplace=True)
    
    transformed_data_filledna = df.to_dict(orient='records')
    
    return transformed_data_filledna

def transform_reviews_data(**kwargs):
    mysql_data = kwargs['task_instance'].xcom_pull(task_ids='extract_task.extract_reviews_task')
    
    transformed_data = []
    for record in mysql_data:
        transformed_record = {
            'review_id': record[0],
            'order_id': record[1],
            'product_id': record[2],
            'product_name': record[3],
            'user_id': record[4],
            'username': record[5],
            'review_score': record[6],
            'review_text': record[7],
            'review_timestamp': record[8],
        }
        transformed_data.append(transformed_record)
        
    df = pd.DataFrame(transformed_data)
    
    dt_string = datetime.now().strftime('%d-%m-%Y %H:%i:%s')
    
    df['order_id'].fillna('N/A', inplace=True)
    df['product_id'].fillna('N/A', inplace=True)
    df['product_name'].fillna('N/A', inplace=True)
    df['user_id'].fillna('N/A', inplace=True)
    df['username'].fillna('N/A', inplace=True)
    df['review_score'].fillna(0, inplace=True)
    df['review_text'].fillna('N/A', inplace=True)
    df['review_timestamp'].fillna(dt_string, inplace=True)
    
    transformed_data_filledna = df.to_dict(orient='records')
    
    return transformed_data_filledna

#  CREATE TABLE IF NOT EXISTS users (
#    user_id int PRIMARY KEY,
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
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (user_id) DO NOTHING;
    """
    
    parameters = [(record['user_id'], record['username'], record['email'], record['first_name'],
                   record['last_name'], record['mobile_number']) 
                   for record in transformed_data_filledna]
    for parameter in parameters:
        postgres_hook.run(insert_query, parameters=parameter)
        
#  CREATE TABLE IF NOT EXISTS products (
#    product_id int PRIMARY KEY,
#    user_id int,
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
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (product_id) DO NOTHING;
    """
    
    parameters = [(record['product_id'], record['user_id'], record['category_name'], record['product_name'],
                   record['product_description'], record['price']) 
                   for record in transform_data_filledna]
    for parameter in parameters:
        postgres_hook.run(insert_query, parameters=parameter)
        
#  CREATE TABLE IF NOT EXISTS orders (
#    order_id int,
#    user_id int, 
#    product_id int,
#    product_name VARCHAR(255),
#    price double precision,
#    quantity int,
#    total_price double precision,
#    purchase_timestamp timestamp,  
#    payment_timestamp timestamp,
#    PRIMARY KEY (order_id, product_id)
#  );
def load_orders_data(**kwargs):
    transformed_data_filledna = kwargs['task_instance'].xcom_pull(task_ids='transform_task.transform_orders_task')
    
    postgres_hook = PostgresHook(postgres_conn_id='postgres_showhuay')
    
    insert_query = """
    INSERT INTO public.orders (order_id, user_id, product_id, product_name, price, quantity, total_price, purchase_timestamp, payment_timestamp)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (order_id, product_id) DO NOTHING;
    """
    
    parameters = [(record['order_id'], record['user_id'], record['product_id'], record['product_name'], record['price'],
                   record['quantity'], record['total_price'], record['purchase_timestamp'], record['payment_timestamp']) 
                   for record in transformed_data_filledna]
    for parameter in parameters:
        postgres_hook.run(insert_query, parameters=parameter)
        
#  CREATE TABLE IF NOT EXISTS reviews (
#    review_id int,
#    order_id int, 
#    product_id int,
#    product_name VARCHAR(255),
#    user_id int,
#    username VARCHAR(255),
#    review_score int,
#    review_text text,
#    review_timestamp timestamp,
#    PRIMARY KEY (review_id, order_id, product_id)
#  );
def load_reviews_data(**kwargs):
    transformed_data_filledna = kwargs['task_instance'].xcom_pull(task_ids='transform_task.transform_reviews_task')
    
    postgres_hook = PostgresHook(postgres_conn_id='postgres_showhuay')
    
    insert_query = """
    INSERT INTO public.reviews (review_id, order_id, product_id, product_name, user_id, username, review_score, review_text, review_timestamp)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (review_id, order_id, product_id) DO NOTHING;
    """
    
    parameters = [(record['review_id'], record['order_id'], record['product_id'], record['product_name'], record['user_id'],
                   record['username'], record['review_score'], record['review_text'], record['review_timestamp'])
                   for record in transformed_data_filledna]
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
            sql="""
            SELECT 
            user_id, username, email, fname, lname, telephone_number 
            FROM 
            user;
            """,
            dag=dag,
        )
        
        extract_products_task = MySqlOperator(
            task_id='extract_products_task',
            mysql_conn_id='mysql_showhuay',
            sql="""
            SELECT 
            product_id, user_id, category_id, product_name, product_description, price 
            FROM 
            product;
            """,
            dag=dag,
        )
        
        extract_orders_task = MySqlOperator(
            task_id='extract_orders_task',
            mysql_conn_id='mysql_showhuay',
            sql="""
            SELECT 
            p1.purchase_id, p1.user_id, p2.product_id, p3.product_name, p3.price, 
            p2.quantity, p3.price * p2.quantity AS total_price, 
            DATE_FORMAT(p1.purchase_timestamp, '%d-%m-%Y %H:%i:%s') AS formatted_purchase_timestamp,
            DATE_FORMAT(p1.payment_timestamp, '%d-%m-%Y %H:%i:%s') AS formatted_payment_timestamp
            FROM 
            purchase p1
            LEFT JOIN 
            purchase_product p2 on p1.purchase_id = p2.purchase_id
            LEFT JOIN 
            product p3 on p2.product_id = p3.product_id;
            """,
            dag=dag,
        )
        
        extract_reviews_task = MySqlOperator(
            task_id='extract_reviews_task',
            mysql_conn_id='mysql_showhuay',
            sql="""
            SELECT
            p1.review_id, pp.purchase_id, p1.product_id, p2.product_name, p1.user_id,
            u.username, p1.review_score, p1.review_text,
            DATE_FORMAT(p1.review_timestamp, '%d-%m-%Y %H:%i:%s') AS formatted_review_timestamp
            FROM
            product_review p1
            LEFT JOIN
            purchase_product pp on p1.product_id = pp.product_id
            LEFT JOIN
            product p2 on p1.product_id = p2.product_id
            LEFT JOIN
            user u on p1.user_id = u.user_id;
            """,
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
        
        transform_orders_task = PythonOperator(
            task_id='transform_orders_task',
            python_callable=transform_orders_data,
            dag=dag,
        )
        
        transform_reviews_task = PythonOperator(
            task_id='transform_reviews_task',
            python_callable=transform_reviews_data,
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
        
        load_orders_task = PythonOperator(
            task_id='load_orders_task',
            python_callable=load_orders_data,
            dag=dag,
        )
        
        load_reviews_task = PythonOperator(
            task_id='load_reviews_task',
            python_callable=load_reviews_data,
            dag=dag,
        )
    
    extract_task >> transform_task >> load_task