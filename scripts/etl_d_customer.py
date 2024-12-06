import apache_beam as beam
import json
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
import argparse

def clean_customer_data(row):
    try:
        cols = row.split(",")
        
        # Ensure we have enough columns
        if len(cols) < 5:
            print(f"Skipping invalid row (not enough columns): {row}")
            return None
        
        # Extract columns with strip to remove leading/trailing whitespace
        customer_id = cols[0].strip().replace("\"","")
        customer_unique_id = cols[1].strip().replace("\"","")
        customer_zip_code_prefix = cols[2].strip().replace("\"","")
        customer_city = cols[3].strip().replace("\"","")
        customer_state = cols[4].strip().replace("\"","")
        
        # Return cleaned data
        return {
            'customer_id': customer_id,
            'customer_unique_id': customer_unique_id,
            'customer_zip_code_prefix': customer_zip_code_prefix,
            'customer_city': customer_city,
            'customer_state': customer_state
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
            {'name': 'customer_id', 'type': 'STRING'},
            {'name': 'customer_unique_id', 'type': 'STRING'},
            {'name': 'customer_zip_code_prefix', 'type': 'STRING'},
            {'name': 'customer_city', 'type': 'STRING'},
            {'name': 'customer_state', 'type': 'STRING'},
        ]
    }
    
    # Construct the full table reference
    table_ref = "deft-approach-440005-r6:staging.d_customer"

    # Create the pipeline
    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | 'ReadInput' >> beam.io.ReadFromText(path_args.input, skip_header_lines=1)
            | 'RemoveDuplicates' >> beam.Distinct()
            | 'Clean Data' >> beam.Map(clean_customer_data)
            # Combined filter to check multiple conditions
            | 'Filter Meaningful Rows' >> beam.Filter(
                lambda x: x and x.get('customer_id') is not None and 
                        x.get('customer_unique_id') is not None
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