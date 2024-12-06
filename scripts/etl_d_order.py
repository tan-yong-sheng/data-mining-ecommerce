import apache_beam as beam
import json
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
import argparse
from datetime import datetime
import re

def standardize_timestamp(date_str):
    if not date_str or date_str.strip() == '':
        return None
    
    # Remove any extra whitespace
    date_str = date_str.strip()
    
    # List of potential input formats to try
    date_formats = [
        '%Y-%m-%d %H:%M:%S',     # DD-MM-YYYY HH:MM:SS
        '%Y/%m/%d %H:%M:%S',     # DD/MM/YYYY HH:MM:SS
        '%Y.%m.%d %H:%M:%S',     # DD.MM.YYYY HH:MM:SS
    ]

    for fmt in date_formats:
        try:
            # Parse the date using the current format
            parsed_date = datetime.strptime(date_str, fmt)
            
            # Convert to the standard format
            return parsed_date.strftime('%Y-%m-%d %H:%M:%S')
        except ValueError:
            continue
    # If no format works, print detailed error
    print(f"Could not parse date: '{date_str}'")
    return None


def clean_order_data(row):
    try:
        cols = row.split(",")
        
        # Ensure we have enough columns
        if len(cols) < 8:
            print(f"Skipping invalid row (not enough columns): {row}")
            return None
        
        # Extract columns with strip to remove leading/trailing whitespace
        order_id = cols[0].strip()
        customer_id = cols[1].strip()
        order_status = cols[2].strip()
        order_purchase_timestamp = cols[3].strip()
        order_approved_at = cols[4].strip()
        ship_date = cols[5].strip()
        delivery_date = cols[6].strip()
        estimated_delivery_date = cols[7].strip()
        
        # Parse date columns
        parsed_order_purchase_timestamp = standardize_timestamp(order_purchase_timestamp)
        parsed_order_approved_at = standardize_timestamp(order_approved_at)
        parsed_order_delivered_carrier_date = standardize_timestamp(ship_date)
        parsed_order_delivery_date = standardize_timestamp(delivery_date)
        parsed_order_estimated_delivery_date = standardize_timestamp(estimated_delivery_date)
        
        # Return cleaned data
        return {
            'order_id': order_id,
            'customer_id': customer_id,
            'order_status': order_status,
            'order_purchase_timestamp': parsed_order_purchase_timestamp,
            'order_approved_at': parsed_order_approved_at,
            'order_delivered_carrier_date': parsed_order_delivered_carrier_date,
            'order_delivered_customer_date': parsed_order_delivery_date,
            'order_estimated_delivery_date': parsed_order_estimated_delivery_date,
        }
    except Exception as e:
        print(f"Error processing row: {row}")
        print(f"Error details: {str(e)}")
        return None


def run(argv=None):
    # Create an argument parser
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input",
        dest="input",
        required=True,
        help="Input file to process."
    )
    parser.add_argument(
        "--temp_location",
        dest="temp_location",
        required=True,
        help="GCS temp location for BigQuery file loading"
    )
    parser.add_argument(
        "--runner",
        dest="runner",
        required=False,
        help="Pipeline runner (e.g., DirectRunner or DataflowRunner)."
    )
    path_args, pipeline_args = parser.parse_known_args(argv)
    
    # Create pipeline options
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(StandardOptions).runner = path_args.runner or 'DirectRunner'
    
    # Define the BigQuery table schema
    table_schema = {
        'fields': [
            {'name': 'order_id', 'type': 'STRING'},
            {'name': 'customer_id', 'type': 'STRING'},
            {'name': 'order_status', 'type': 'STRING'},
            {'name': 'order_purchase_timestamp', 'type': 'TIMESTAMP'},
            {'name': 'order_approved_at', 'type': 'TIMESTAMP'},
            {'name': 'order_delivered_carrier_date', 'type': 'TIMESTAMP'},
            {'name': 'order_delivered_customer_date', 'type': 'TIMESTAMP'},
            {'name': 'order_estimated_delivery_date', 'type': 'TIMESTAMP'},
        ]
    }
    
    # Construct the full table reference
    table_ref = "deft-approach-440005-r6:staging.d_order"
    
    # Create the pipeline
    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | 'ReadInput' >> beam.io.ReadFromText(path_args.input, skip_header_lines=1)
            | 'RemoveDuplicates' >> beam.Distinct()
            | 'Clean Data' >> beam.Map(clean_order_data)
            | 'Filter Meaningful Rows' >> beam.Filter(
                lambda x: x and x.get('order_id') is not None and  
                        x.get('customer_id') is not None
            )
            | 'WriteToBigQuery' >> WriteToBigQuery(
                table_ref,
                schema=table_schema,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=BigQueryDisposition.WRITE_TRUNCATE,
                custom_gcs_temp_location=path_args.temp_location
            )
        )

if __name__ == '__main__':
    run()