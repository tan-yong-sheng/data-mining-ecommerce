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
    'd_product_dag',
    default_args=default_args,
    schedule_interval='@weekly',
    catchup=False,
) as dag:

    # Task to run the Dataflow job
    t1 = DataflowCreatePythonJobOperator(
        task_id="etl_product_data",
        py_file="gs://us-central1-airflow-0e94d344-bucket/scripts/etl_d_product.py",
        options={
            "input": "gs://ecommerce-data-mining/dataset/d_product/d_product.csv",
            "temp_location": "gs://ecommerce-data-mining/dataset/d_product/temp_dir"
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
        task_id="upsert_product_to_bigquery",
        configuration={
            "query": {
                "query": """
                      MERGE INTO `deft-approach-440005-r6.dimensions.d_product` AS target
                      USING `deft-approach-440005-r6.staging.d_product` AS source
                      ON
                        source.product_id = target.product_id
                      WHEN MATCHED THEN
                        UPDATE SET
                          product_category = source.product_category,
                          segment = source.segment
                      WHEN NOT MATCHED BY TARGET THEN
                        INSERT (
                          product_id,
                          product_category,
                          segment
                        )
                        VALUES (
                          source.product_id,
                          source.product_category,
                          source.segment
                        );

                """,
                "useLegacySql": False,
            }
        },
        location="US",
        gcp_conn_id="google_cloud_default",  # Ensure this connection exists in Airflow
    )


    # Define the task dependencies
    t1 >> t2