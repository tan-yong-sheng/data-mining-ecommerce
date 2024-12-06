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
    'd_order_dag',
    default_args=default_args,
    schedule_interval='@weekly',
    catchup=False,
) as dag:

    # Task to run the Dataflow job
    t1 = DataflowCreatePythonJobOperator(
        task_id="etl_order_data",
        py_file="gs://us-central1-airflow-0e94d344-bucket/scripts/etl_d_order.py",
        options={
            "input": "gs://ecommerce-data-mining/dataset/d_order/d_order.csv",
            "temp_location": "gs://ecommerce-data-mining/dataset/d_order/temp_dir"
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
        task_id="upsert_order_to_bigquery",
        configuration={
            "query": {
                "query": """
                    MERGE INTO dimensions.d_order AS target
                    USING staging.d_order AS source
                    ON target.order_id = source.order_id
                    WHEN MATCHED THEN
                      UPDATE SET
                        customer_id = source.customer_id,
                        order_status = source.order_status,
                        order_purchase_timestamp = source.order_purchase_timestamp,
                        order_approved_at = source.order_approved_at,
                        order_delivered_carrier_date = source.order_delivered_carrier_date,
                        order_delivered_customer_date = source.order_delivered_customer_date,
                        order_estimated_delivery_date = source.order_estimated_delivery_date
                    WHEN NOT MATCHED THEN
                      INSERT (order_id, customer_id, order_status, order_purchase_timestamp, order_approved_at, order_delivered_carrier_date, order_delivered_customer_date, order_estimated_delivery_date)
                      VALUES (source.order_id, source.customer_id, source.order_status, source.order_purchase_timestamp, source.order_approved_at, source.order_delivered_carrier_date, source.order_delivered_customer_date, source.order_estimated_delivery_date);
                """,
                "useLegacySql": False,
            }
        },
        location="US",
        gcp_conn_id="google_cloud_default",  # Ensure this connection exists in Airflow
    )


    # Define the task dependencies
    t1 >> t2