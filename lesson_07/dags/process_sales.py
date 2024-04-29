from airflow import DAG
from datetime import datetime, timedelta
import os
import json

from airflow.operators.branch import BaseBranchOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.models import TaskInstance, Variable
from airflow.utils.context import Context


default_args = {
    'owner': 'danylyshyn',
    'depends_on_past': True,
    #'start_date': datetime(2024, 4, 29),
    #'start_date': datetime(2022, 8, 9),
    'email': ['vdanko07@ukr.net'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

BASE_DIR = os.getenv("BASE_DIR", "D:\\LABA\\RD\\salesdata")


class RateBranchOperator(BaseBranchOperator):
    def choose_branch(self, context: Context):
        ti = context['ti']
        api_status_code = ti.xcom_pull(task_ids='extract_data_from_api', key='extract_data_from_api_status_code')
        if api_status_code == 201:
            return 'convert_to_avro'
        else:
            return 'send_email_alert'


def extract_data_from_api(**kwargs):
    ti: TaskInstance = kwargs.get('ti')

    today = kwargs['dag_run'].logical_date.strftime('%Y-%m-%d')
    print(f"Start:  convert_to_avro {today}")
    RAW_DIR = os.path.join(BASE_DIR, "raw", "sales", today)

    print("Start:  extract_data_from_api")
    hook = HttpHook(
        method='POST',
        http_conn_id='extract_data_from_api'
    )
    json_body = json.dumps({"date": today,"raw_dir": RAW_DIR})
    print(f"json_body: {json_body}")

    response = hook.run('', json_body, {"Content-Type": "application/json"})
    print(f"End:  extract_data_from_api {response.status_code}")

    ti.xcom_push('extract_data_from_api_status_code', response.status_code)

    assert response.status_code == 201

    return response.status_code == 201


def convert_to_avro(**kwargs):
    ti: TaskInstance = kwargs.get('ti')

    today = kwargs['dag_run'].logical_date.strftime('%Y-%m-%d')
    print(f"Start:  convert_to_avro {today}")
    RAW_DIR = os.path.join(BASE_DIR, "raw", "sales", today)
    STG_DIR = os.path.join(BASE_DIR, "stg", "sales", today)

    print("Start: convert_to_avro")
    hook = HttpHook(
        method='POST',
        http_conn_id='convert_to_avro'
    )
    json_body = json.dumps(dict(raw_dir=RAW_DIR, stg_dir=STG_DIR))

    response = hook.run('',  json_body,{"Content-Type": "application/json"})
    print(f"End: convert_to_avro {response.status_code}")

    ti.xcom_push('extract_data_from_api_status_code', response.status_code)

    assert response.status_code == 201

    return response.status_code == 201


with DAG(
    dag_id='process_sales',
    start_date=datetime(2022, 8, 9),
    end_date=datetime(2022, 8, 12),
    schedule_interval="0 1 * * *",
    # schedule_interval="@daily" # <-- after midnight,
    catchup=True,
    default_args=default_args,
    max_active_runs=1,
    tags=['sales','lesson_07']
) as dag:

    start = EmptyOperator(
        task_id='start',
    )

    extract_data_task = PythonOperator(
        task_id='extract_data_from_api',
        python_callable=extract_data_from_api,
    )

    convert_to_avro_task = PythonOperator(
        task_id='convert_to_avro',
        python_callable=convert_to_avro,
    )

    send_email_alert_task = EmailOperator(
        task_id='send_email_alert',
        to='danylyshyn@gmail.com',
        subject='Airflow Alert Email',
        html_content="""<h1>ALERT!</h1><p>Extract data from api error!</p>"""
    )

    status_code_branch = RateBranchOperator(
        task_id='status_code_branch'
    )

    start >> extract_data_task >> status_code_branch

    status_code_branch >> convert_to_avro_task
    status_code_branch >> send_email_alert_task

