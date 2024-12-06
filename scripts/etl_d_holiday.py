from datetime import datetime
import apache_beam as beam
import urllib.request
import json
import argparse
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition

def standardize_date(date_str):
    if not date_str or date_str.strip() == '':
        return None
    
    # Remove any extra whitespace
    date_str = date_str.strip()
    
    # List of potential input formats to try
    date_formats = [
        '%d-%m-%Y',     # DD-MM-YYYY
        '%d/%m/%Y',     # DD/MM/YYYY
        '%d.%m.%Y',     # DD.MM.YYYY
    ]

    for fmt in date_formats:
        try:
            # Parse the date using the current format
            parsed_date = datetime.strptime(date_str, fmt)
            
            # Convert to the standard format
            return parsed_date.strftime('%Y-%m-%d')
        except ValueError:
            continue
    # If no format works, print detailed error
    print(f"Could not parse date: '{date_str}'")
    return None


def parse_holiday_row(row):
    """Parse and validate each row before loading to BigQuery"""
    try:
        cols = row.split(',')  # Adjust delimiter as needed
        # Split the row and validate
        if len(cols) < 2:
            return None
        date = cols[0].strip()
        name = cols[1].strip()

        # parse date column
        parsed_date = standardize_date(date)     

        return {'date': parsed_date, 'name': name}
    except Exception as e:
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
            {'name': 'date', 'type': 'DATE'},
            {'name': 'name', 'type': 'STRING'}
        ]
    }

    # Construct the full table reference
    table_ref = "deft-approach-440005-r6:staging.d_holiday"

    # Create the Apache Beam pipeline
    with beam.Pipeline(options=pipeline_options) as p:
        # The pipeline:
        (p
            | 'ReadInput' >> beam.io.ReadFromText(path_args.input, skip_header_lines=1)
            | 'RemoveDuplicates' >> beam.Distinct()
            | 'ParseRows' >> beam.Map(parse_holiday_row)
            | 'FilterValidRows' >> beam.Filter(lambda x: x is not None)
            | 'WriteHolidaysToBigQuery' >> WriteToBigQuery(
                table_ref,
                schema=table_schema,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=BigQueryDisposition.WRITE_TRUNCATE,
                custom_gcs_temp_location=path_args.temp_location
              )
        )

if __name__ == '__main__':
    run()
