from airflow import DAG
from datetime import datetime
from datetime import timedelta
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator


# Constants
BUCKET_NAME = "vdanko_bucket_1"
GCS_FOLDER = "src1/sales/v1/{{ data_description(ds) }}"
CURRENT_DATE = "{{ ds }}"
LOCAL_FILE_PATH = "/data/sales_clean.csv"
DESTINATION_FILE_PATH = GCS_FOLDER + "{{ ds }}.csv"


def data_description(ds_date):
    date_list = ds_date.split('-')
    return f"year={date_list[0]}/month={date_list[1]}/day={date_list[2]}/"


default_args = {
    'owner': 'danylyshyn',
    'depends_on_past': True,
    'email': ['vdanko07@ukr.net'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
        dag_id='process_gcloud',
        start_date=datetime(2022, 8, 1),
        end_date=datetime(2022, 8, 3),
        schedule_interval="0 1 * * *",
        user_defined_macros={
            "ds_date": CURRENT_DATE,  # Macro can be a variable
            "data_description": data_description,  # Macro can also be a function
        },
        catchup=True,
        default_args=default_args,
        max_active_runs=1,
        tags=['gcloud', 'lesson_10']
) as dag:
    start = EmptyOperator(
        task_id='start',
    )

    upload_file_to_gsc = LocalFilesystemToGCSOperator(
        task_id='upload_file_to_gsc',
        src=LOCAL_FILE_PATH,
        dst=DESTINATION_FILE_PATH,
        bucket=BUCKET_NAME,
        mime_type='text/csv'
    )

    end = EmptyOperator(
        task_id='end',
    )

    start >> upload_file_to_gsc >> end
