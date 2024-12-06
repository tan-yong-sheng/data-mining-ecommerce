from airflow import models
from airflow.providers.google.cloud.operators.dataflow import DataflowCreatePythonJobOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime, timedelta

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 11, 1),  # Must be in the past
    'retries': 1,
    'retry_delay': timedelta(seconds=50),
}

# Define the DAG
with models.DAG(
    'd_customer_dag',
    default_args=default_args,
    schedule_interval='@weekly',
    catchup=False,
) as dag:

    # Task to run the Dataflow job
    t1 = DataflowCreatePythonJobOperator(
        task_id="etl_customer_data",
        py_file="gs://us-central1-airflow-0e94d344-bucket/scripts/etl_d_customer.py",
        options={
            "input": "gs://ecommerce-data-mining/dataset/d_customer/d_customer.csv",
            "temp_location": "gs://ecommerce-data-mining/dataset/d_customer/temp_dir"
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
        task_id="upsert_customer_to_bigquery",
        configuration={
            "query": {
                "query": """
                    MERGE INTO dimensions.d_customer AS target
                    USING staging.d_customer AS source
                    ON target.customer_id = source.customer_id AND target.customer_unique_id = source.customer_unique_id
                    WHEN MATCHED THEN
                    UPDATE SET
                        customer_id = source.customer_id,
                        customer_unique_id = source.customer_unique_id,
                        customer_zip_code_prefix = source.customer_zip_code_prefix,
                        customer_city = source.customer_city,
                        customer_state = source.customer_state
                    WHEN NOT MATCHED THEN
                    INSERT (customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state)
                    VALUES (source.customer_id, source.customer_unique_id, source.customer_zip_code_prefix, source.customer_city, source.customer_state);
                """,
                "useLegacySql": False,
            }
        },
        location="US",
        gcp_conn_id="google_cloud_default",  # Ensure this connection exists in Airflow
    )

    # Define the task dependencies
    t1 >> t2