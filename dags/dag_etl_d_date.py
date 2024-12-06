from airflow import models
from airflow.providers.google.cloud.operators.dataflow import DataflowCreatePythonJobOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime, timedelta

next_year = "{{ macros.datetime.now().year + 1 }}"

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 12, 25),
    'retries': 1,
    'retry_delay': timedelta(seconds=50),
}

# Define the DAG
with models.DAG(
    'd_date_dag',
    default_args=default_args,
    schedule_interval='@yearly', # Run the DAG every year
    catchup=False,
) as dag:


    # Task to run the Dataflow job
    t1 = DataflowCreatePythonJobOperator(
        task_id="etl_date_data",
        py_file="gs://us-central1-airflow-0e94d344-bucket/scripts/etl_d_holiday.py",
        options={
            "input": "gs://ecommerce-data-mining/dataset/d_holiday/d_holiday.csv",
            "temp_location": "gs://ecommerce-data-mining/dataset/d_holiday/temp_dir"
        },
        dataflow_default_options={
            "project": "deft-approach-440005-r6",
            "region": "us-central1-c",
            "runner": "DataFlowRunner"
        },
        gcp_conn_id="google_cloud_default"  # Ensure this connection exists in Airflow
    )


    # Task to write data to BigQuery
    t2 = BigQueryInsertJobOperator(
        task_id="upsert_holiday_to_bigquery",
        configuration={
            "query": {
                "query": """
                    MERGE INTO raw.d_holiday AS target
                    USING staging.d_holiday AS source
                    ON target.date = source.date AND target.name = source.name
                    WHEN NOT MATCHED BY TARGET THEN
                    INSERT (date, name)
                    VALUES (source.date, source.name);
                """,
                "useLegacySql": False,
            }
        },
        location="US",
        gcp_conn_id="google_cloud_default",  # Ensure this connection exists in Airflow
    )

    # Task to create d_date table
    t3 = BigQueryInsertJobOperator(
        task_id="create_d_date",
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE TABLE raw.d_date
                    PARTITION BY DATE_TRUNC(Date, MONTH) AS 
                    SELECT
                    Date,
                    DATE_TRUNC(Date, MONTH) AS first_day_of_the_month,
                    EXTRACT(YEAR FROM Date) AS year,
                    EXTRACT(WEEK FROM Date) AS week,
                    EXTRACT(DAY FROM Date) AS day,
                    FORMAT_DATE('%Q', Date) as quarter,
                    EXTRACT(MONTH FROM Date) AS month,
                    IF(FORMAT_DATE('%A', Date) IN ('Saturday', 'Sunday'), FALSE, TRUE) AS is_weekday,
                    CASE 
                    WHEN dh.date IS NOT NULL THEN TRUE
                    ELSE FALSE
                    END AS is_holiday
                    FROM UNNEST(
                    GENERATE_DATE_ARRAY('2016-01-01', '{next_year}-12-31', INTERVAL 1 DAY)
                    ) AS Date
                    LEFT JOIN raw.d_holiday dh ON Date = dh.date;
                """,
                "useLegacySql": False,
            }
        },
        location="US",
        gcp_conn_id="google_cloud_default",  # Ensure this connection exists in Airflow
    )



    # Define the task dependencies
    t1 >> t2 >> t3
