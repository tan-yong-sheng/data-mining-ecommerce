import apache_beam as beam
import json
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
import argparse


def clean_product_data(row):
    try:
        cols = row.split(',')
        if len(cols) < 3:
            return None

        product_id = cols[0]
        product_category = cols[1].strip() if len(cols) > 1 else ''
        segment = cols[2].strip()

        # Category normalization
        category_mapping = {
            'home_appliances_2': 'home_appliances',
            'home_comfort_2': 'home_comfort',
            '#N/A': 'Others'
        }
        product_category = category_mapping.get(product_category, product_category)

        return {'product_id': product_id, 'product_category': product_category, 'segment': segment}
    except Exception:
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
            {'name': 'product_id', 'type': 'STRING'},
            {'name': 'product_category', 'type': 'STRING'},
            {'name': 'segment', 'type': 'STRING'},
        ]
    }

    # Construct the full table reference
    table_ref = "deft-approach-440005-r6:staging.d_product"

    # Create the pipeline
    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | 'ReadInput' >> beam.io.ReadFromText(path_args.input, skip_header_lines=1)
            | 'RemoveDuplicates' >> beam.Distinct()
            | 'CleanData' >> beam.Map(clean_product_data)
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
