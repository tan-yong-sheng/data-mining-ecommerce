import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
import argparse
from datetime import datetime

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



def clean_order_item_data(row):
    try:
        cols = row.split(",")
        
        # Ensure we have enough columns
        if len(cols) < 5:
            print(f"Skipping invalid row (not enough columns): {row}")
            return None
        
        # Extract columns with strip to remove leading/trailing whitespace
        order_id = cols[0].strip()
        order_item_id = cols[1].strip()
        order_purchase_timestamp = cols[2].strip()
        order_approved_at = cols[3].strip()
        customer_id = cols[4].strip()
        product_id = cols[5].strip()
        price = cols[6].strip()
        freight_value = cols[7].strip()
        order_status = cols[8].strip()

        # Parse date columns
        parsed_order_purchase_timestamp = standardize_timestamp(order_purchase_timestamp)
        parsed_order_approved_at = standardize_timestamp(order_approved_at)

        # Return cleaned data
        return {
            'order_id': order_id,
            'order_item_id': int(order_item_id),
            'order_purchase_timestamp': parsed_order_purchase_timestamp ,
            'order_approved_at':  parsed_order_approved_at,
            'customer_id': customer_id,
            'product_id': product_id,
            'price': round(float(price),2),
            'freight_value': round(float(freight_value),2),
            'order_status': order_status
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
            {'name': 'order_item_id', 'type': 'STRING'},
            {'name': 'order_purchase_timestamp', 'type': 'TIMESTAMP'},
            {'name': 'order_approved_at', 'type': 'TIMESTAMP'},
            {'name': 'customer_id', 'type': 'STRING'},            
            {'name': 'product_id', 'type': 'STRING'},
            {'name': 'price', 'type': 'FLOAT'},
            {'name': 'freight_value', 'type': 'FLOAT'},
            {'name': 'order_status', 'type': 'STRING'}
        ]
    }
    
    # Construct the full table reference
    table_ref = "deft-approach-440005-r6:staging.d_order_item"

    # Create the pipeline
    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | 'ReadInput' >> beam.io.ReadFromText(path_args.input, skip_header_lines=1)
            | 'RemoveDuplicates' >> beam.Distinct()
            | 'Clean Data' >> beam.Map(clean_order_item_data)
            # Combined filter to check multiple conditions
            | 'Filter Meaningful Rows' >> beam.Filter(
                lambda x: x and x.get('order_id') is not None and 
                        x.get('order_item_id') is not None and 
                        x.get('product_id') is not None
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