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
    'd_order_item_dag',
    default_args=default_args,
    schedule_interval='@weekly',
    catchup=False,
) as dag:

    # Task to run the Dataflow job
    t1 = DataflowCreatePythonJobOperator(
        task_id="etl_order_item_data",
        py_file="gs://us-central1-airflow-0e94d344-bucket/scripts/etl_d_order_item.py",
        options={
            "input": "gs://ecommerce-data-mining/dataset/d_order_item/d_order_item.csv",
            "temp_location": "gs://ecommerce-data-mining/dataset/d_order_item/temp_dir"
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
        task_id="upsert_order_item_to_bigquery",
        configuration={
            "query": {
                "query": """
                        MERGE INTO dimensions.d_order_item AS target
                        USING staging.d_order_item AS source
                        ON target.order_id = source.order_id AND target.order_item_id = source.order_item_id
                            AND target.product_id = source.product_id
                        WHEN MATCHED THEN
                        UPDATE SET
                            order_id = source.order_id,
                            order_item_id = source.order_item_id,
                            order_purchase_timestamp = source.order_purchase_timestamp,
                            order_approved_at = source.order_approved_at,
                            customer_id = source.customer_id,
                            product_id = source.product_id,
                            price = source.price,
                            freight_value = source.freight_value,
                            order_status = source.order_status
                        WHEN NOT MATCHED THEN
                        INSERT (order_id, order_item_id, order_purchase_timestamp, order_approved_at, 
                                customer_id, product_id, price, freight_value, order_status)
                        VALUES (source.order_id, source.order_item_id, source.order_purchase_timestamp, 
                            source.order_approved_at, source.customer_id, source.product_id, source.price, 
                            source.freight_value, source.order_status);
                """,
                "useLegacySql": False,
            }
        },
        location="US",
        gcp_conn_id="google_cloud_default",  # Ensure this connection exists in Airflow
    )


    # Define the task dependencies
    t1 >> t2