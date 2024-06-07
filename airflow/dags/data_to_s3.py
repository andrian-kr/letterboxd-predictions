from io import StringIO
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import pandas as pd
import logging
import os

logging.basicConfig(level=logging.DEBUG)

S3_BUCKET = 'mlops-ucu-2024'
S3_KEY = 'popular_movies.csv'
S3_CONN_ID = 'aws_s3'

default_args = {
    'owner': 'airflow',
    'e-mail': 'andriankrav@gmail.com',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 5),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'execution_timeout': timedelta(minutes=5),
}


def preprocess_and_upload_to_s3(local_file_path, s3_bucket, s3_key, aws_conn_id):
    try:
        s3_hook = S3Hook(aws_conn_id)
        logging.info('Connected to S3')

        file_exists = s3_hook.check_for_key(s3_key, s3_bucket)
        logging.info(f'File exists: {file_exists}')

        if file_exists:
            existing_file_obj = s3_hook.get_key(s3_key, s3_bucket)
            string_data = existing_file_obj.get()['Body'].read().decode('utf-8')
            existing_df = pd.read_csv(StringIO(string_data))
            logging.info(f'Successfully read: {existing_df.info()}')
        else:
            existing_df = pd.DataFrame()

        new_df = pd.read_csv(local_file_path)
        logging.info(f'New df: {new_df.info()}')

        merged_df = new_df if existing_df.empty else pd.concat([existing_df, new_df]).drop_duplicates(
            subset=['Id'], keep='first')

        # Save the merged DataFrame to a temporary file
        merged_file_path = '/tmp/merged_popular_movies.csv'
        merged_df.to_csv(merged_file_path, index=False)
        logging.info(f'Created temp file')

        # Upload the merged file back to S3
        s3_hook.load_file(filename=merged_file_path, key=s3_key,
                          bucket_name=s3_bucket, replace=True)
        logging.info(f"Merged file uploaded to s3://{s3_bucket}/{s3_key}")
    except Exception as e:
        logging.error(f'Exception: {repr(e)}')


with DAG(
    'preprocess_and_upload_movies',
    default_args=default_args,
    description='Preprocess popular movies CSV and upload to S3',
    schedule_interval='@weekly',
    catchup=False,
) as dag:
    preprocess_and_upload_task = PythonOperator(
        task_id='preprocess_and_upload_csv_to_s3',
        python_callable=preprocess_and_upload_to_s3,
        op_kwargs={
            'local_file_path': '/opt/airflow/data/popular_movies.csv',
            's3_bucket': S3_BUCKET,
            's3_key': S3_KEY,
            'aws_conn_id': S3_CONN_ID,
        },
        dag=dag,
    )
    preprocess_and_upload_task
