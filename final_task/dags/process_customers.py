from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator


default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'email': ['vdanko07@ukr.net'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
}

DATASET_NAME = "de-07-vasyl-danylyshyn"
DSBRONZE = "bronze"
DSSLIVER = "silver"
DSGOLD = "gold"
GCBUCKET = "vdanko_final_data"


with DAG(
    dag_id='process_customers',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    start = EmptyOperator(
        task_id='start',
    )


    load_bronze = GCSToBigQueryOperator(
        task_id='load_bronze',
        bucket=f"{GCBUCKET}",
        source_objects=['customers/*.csv'],
        destination_project_dataset_table=f"{DATASET_NAME}.{DSBRONZE}.customers",
        schema_fields=[
            {'name': 'Id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'FirstName', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'LastName', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Email', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'RegistrationDate', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'State', 'type': 'STRING', 'mode': 'NULLABLE'},
        ],
        write_disposition='WRITE_TRUNCATE',
        source_format='CSV',
        skip_leading_rows=1,
    )


    create_silver = BigQueryCreateEmptyTableOperator(
        task_id='create_silver_table',
        dataset_id='silver',
        table_id='customers',
        schema_fields=[
            {'name': 'client_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'first_name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'last_name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'email', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'registration_date', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
            {'name': 'state', 'type': 'STRING', 'mode': 'NULLABLE'},
        ],
    )


    copy_to_silver = BigQueryInsertJobOperator(
        task_id='copy_to_silver',
        configuration={
            "query": {
                "query": """
                  CREATE OR REPLACE TABLE `de-07-vasyl-danylyshyn.silver.customers` AS
                  SELECT
                    DISTINCT 
                    Id AS client_id,
                    FirstName AS first_name,
                    LastName AS last_name,
                    Email AS email,
                    IFNULL(
                       SAFE.PARSE_TIMESTAMP('%Y-%m-%d', RegistrationDate),
                       SAFE.PARSE_TIMESTAMP('%Y-%b-%d', RegistrationDate)
                    ) AS registration_date,
                    State AS state
                  FROM
                      `de-07-vasyl-danylyshyn.bronze.customers`
                  """,
                "useLegacySql": False,
            }
        },
    )

    start >> load_bronze >> create_silver >> copy_to_silver