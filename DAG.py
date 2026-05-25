'''
=================================================
Milestone 3

Name  : Stefano Veronigo Wijaya
Batch : FTDS-053-RMT

This program automates the process of:
- Fetching raw data from PostgreSQL
- Cleaning the data and saving to CSV
- Posting the clean data to Elasticsearch

Dataset: Superstore Sales Dataset
=================================================
'''

from airflow import DAG
from airflow.operators.python import PythonOperator
import datetime as dt
from datetime import timedelta
import pandas as pd
import psycopg2
from elasticsearch import Elasticsearch

# Fetch data
def fetch_data():
    '''
    Fetch data from PostGreSQL

    Parameters: None
    Return: None    (Saves file locally)

    Example Usage:
    fetch_data()

    '''
    connection = psycopg2.connect(
        user = "airflow",
        password = "airflow",
        host = "postgres",
        port = "5432",
        database = "m3"
    )

    df = pd.read_sql("SELECT * FROM table_m3", connection)
    df.to_csv('/opt/airflow/dags/P2M3_Stefano_Veronigo_Wijaya_data_raw.csv', index=False)

# Clean data
def clean_data():
    '''
    Clean up data (all lowercase, space to _, remove unwanted symbols, handles missing value)

    Parameters: None
    Return: None    (Saves clean data)

    Example Usage:
    clean_data()
    '''
    df = pd.read_csv('/opt/airflow/dags/P2M3_Stefano_Veronigo_Wijaya_data_raw.csv')
    df = df.drop_duplicates()     # Remove duplicates
    df.columns = (      # Clean up column names
        df.columns
        .str.lower()    # Change all letter to lower letters
        .str.strip()    # Remove external spaces
        .str.replace(" ", "_")  # Replace spaces with _
        .str.replace(r"[^\w_]", "", regex=True)     # Removes unwanted symbols (not needed for the current iteration of the dataset)
    )

    df['order_date'] = pd.to_datetime(df['order_date'], errors = 'coerce')  # Convert string to DateTime for date related columns
    df['ship_date'] = pd.to_datetime(df['ship_date'], errors = 'coerce')

    print(f"Missing values: {df.isnull().sum()}")   # Prints out missing value before cleaning
    num_cols = df.select_dtypes(include=['number']).columns     # Define numerical cols
    df[num_cols] = df[num_cols].fillna(df[num_cols].median())   # Fill missing numeric values with median value
    cat_cols = df.select_dtypes(include=['object']).columns     # Define categorical cols
    for col in cat_cols:
        df[col] = df[col].fillna(df[col].mode()[0])     # Fill missing categorical values with the most frequent class
    print(f"Missing values after cleaning: {df.isnull().sum()}")    # Prints out missing value after cleaning (just in case)

    df.to_csv('/opt/airflow/dags/P2M3_Stefano_Veronigo_Wijaya_data_clean.csv', index = False)   # Save clean data

# Post to elasticsearch
def post_to_elasticsearch():
    '''
    Uploads clean .csv file to ElasticSearch

    Parameters: None
    Return: None

    Example Usage:
    post_to_elasticsearch()
    '''
    es = Elasticsearch("http://elasticsearch:9200")
    print(es.ping())
    df = pd.read_csv('/opt/airflow/dags/P2M3_Stefano_Veronigo_Wijaya_data_clean.csv')
    for i, row in df.iterrows():
        doc = row.to_dict()     # For ElasticSearch to read
        es.index(
            index="superstore_sales",  # Index name in Elasticsearch
            id=i,   # Index num
            body=doc    
        )

# DAG config
default_args = {
    'owner': 'stefano',
    'start_date': dt.datetime(2024, 11, 1) ,    # Starts from November 1st 2024
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=15),
}

with DAG(
    'M3_pipeline',
    default_args=default_args,
    schedule_interval= '10,20,30 9 * * 6',  # cron 
    catchup=False
) as dag:
    fetch = PythonOperator(
        task_id='fetch_from_postgresql',
        python_callable=fetch_data
        )
    
    clean = PythonOperator(
        task_id='data_cleaning',
        python_callable=clean_data
    )

    post = PythonOperator(
        task_id='post_to_elasticsearch',
        python_callable=post_to_elasticsearch
    )

    fetch >> clean >> post