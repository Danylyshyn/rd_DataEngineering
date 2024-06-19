from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator, BigQueryCreateEmptyTableOperator
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

DATASET_NAME = "de-07-vasyl-danylyshyn"
DSBRONZE = "bronze"
DSSLIVER = "silver"
DSGOLD = "gold"
GCBUCKET = "vdanko_final_data"

with DAG(
    dag_id='process_user_profiles',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:


    start = EmptyOperator(
        task_id='start',
    )


    create_silver_table = BigQueryCreateEmptyTableOperator(
        task_id='create_silver_table',
        dataset_id=DSSLIVER,
        table_id="user_profiles",
        schema_fields=[
            {'name': 'user_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'first_name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'last_name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'email', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'birth_date', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
            {'name': 'state', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'phone_number', 'type': 'STRING', 'mode': 'NULLABLE'},
        ],
    )

    load_and_transform_to_silver = GCSToBigQueryOperator(
        task_id='load_and_transform_to_silver',
        bucket='vdanko_final_data',
        source_objects=['user_profiles/*.json'],
        destination_project_dataset_table='de-07-vasyl-danylyshyn.silver.user_profiles',
        schema_fields=[
            {'name': 'user_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'full_name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'email', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'birth_date', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'state', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'phone_number', 'type': 'STRING', 'mode': 'NULLABLE'},
        ],
        source_format='NEWLINE_DELIMITED_JSON',
        write_disposition='WRITE_TRUNCATE',
    )

    # Task 3: Transform the table to split full_name into first_name and last_name
    transform_full_name = BigQueryInsertJobOperator(
        task_id='transform_full_name',
        configuration={
            "query": {
                "query": """
                   CREATE OR REPLACE TABLE `de-07-vasyl-danylyshyn.silver.user_profiles` AS
                   SELECT
                       user_id,
                       SPLIT(full_name, ' ')[SAFE_OFFSET(0)] AS first_name,
                       SPLIT(full_name, ' ')[SAFE_OFFSET(1)] AS last_name,
                       email,
                       IFNULL(
                          SAFE.PARSE_TIMESTAMP('%Y-%m-%d', birth_date),
                          SAFE.PARSE_TIMESTAMP('%Y-%b-%d', birth_date)
                       ) AS birth_date,
                       state,
                       phone_number
                   FROM
                       `de-07-vasyl-danylyshyn.silver.user_profiles`
                   """,
                "useLegacySql": False,
            },
        },
    )

    rich_customers = BigQueryInsertJobOperator(
        task_id='rich_customers',
        configuration={
            "query": {
                "query": """
                CREATE OR REPLACE TABLE `de-07-vasyl-danylyshyn.silver.customers` AS
                SELECT
                    c.client_id,
                    u.first_name,
                    u.last_name,
                    c.email,
                    c.registration_date,
                    u.state
                FROM
                    `de-07-vasyl-danylyshyn.silver.customers` c
                LEFT JOIN
                    `de-07-vasyl-danylyshyn.silver.user_profiles` u
                ON
                    c.email = u.email
                """,
                "useLegacySql": False,
            }
        },
    )

    trigger_enrich_user_profiles = TriggerDagRunOperator(
        task_id='trigger_enrich_user_profiles',
        trigger_dag_id='enrich_user_profiles',
        wait_for_completion=True,
    )

    end = EmptyOperator(
        task_id='end',
    )

    start >> create_silver_table >> load_and_transform_to_silver >> transform_full_name >> rich_customers >> trigger_enrich_user_profiles >> end
