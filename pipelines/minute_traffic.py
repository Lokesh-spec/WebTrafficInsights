import os
import datetime
import argparse
import json
import logging
import typing

import apache_beam as beam
from apache_beam.transforms.combiners import CountCombineFn
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, GoogleCloudOptions

class EventLog(typing.NamedTuple):
    ip: str
    id: str
    lat: float
    lng: float
    user_agent: str
    age_bracket: str
    opted_into_marketing: bool
    http_request: str
    http_response: int
    file_size_bytes: int
    event_datetime: str
    event_ts: int

EventLog.__module__ = __name__


beam.coders.registry.register_coder(EventLog, beam.coders.RowCoder)

additional_bq_parameters = {
    "allowJaggedRows": True,
    "ignoreUnknownValues": True
}


def parse_json(element: str):
    row = json.loads(element)
    # lat/lng sometimes empty string
    if not row["lat"] or not row["lng"]:
        row = {**row, **{"lat": -1, "lng": -1}}
    return EventLog(**row)


def add_timestamp(element: EventLog):
    ts = datetime.datetime.strptime(
        element.event_datetime, "%Y-%m-%dT%H:%M:%S.%f%z"
    ).timestamp()
    return beam.window.TimestampedValue(element, ts)


class AddWindowTS(beam.DoFn):
    def process(self, element: int, window=beam.DoFn.WindowParam):
        
        window_start = window.start.to_utc_datetime().isoformat(timespec="seconds")
        window_end = window.end.to_utc_datetime().isoformat(timespec="seconds")
        logging.info(f"{window_start}")
        logging.info(f"{window_end}")
        output = {
            "window_start": window_start,
            "window_end": window_end,
            "page_views": element,
        }
        yield output

class FormatForBigQuery(beam.DoFn):
    def process(self, element):
        # Output formatted dictionary for BigQuery
        yield {
            "window_start": element["window_start"],
            "window_end": element["window_end"],
            "page_views": element["page_views"],
        }

# Load BigQuery schema from resources/bigquery_schema.json
def load_bigquery_schema(PARENT_DIR):
    schema_path = os.path.join(PARENT_DIR, "resources/bigquery_schema.json")
    
    with open(schema_path, "r") as schema_file:
        schema = json.load(schema_file)
    return schema


def run():
    parser = argparse.ArgumentParser(description="Beam pipeline arguments")
    parser.add_argument(
        "--inputs",
        default="inputs",
        help="Specify folder name that event records are saved",
    )
    parser.add_argument(
        "--runner", default="DirectRunner", help="Specify Apache Beam Runner"
    )
    
    parser.add_argument(
        "--staging_location", default="gs://webapp-logs-2024/staging/", help="GCS Bucket - Staging Location"
    )
    
    parser.add_argument(
        "--temp_location", default="gs://webapp-logs-2024/temp/", help="GCS Bucket - Temp Location"
    )
    
    parser.add_argument(
        "--project", default="kinetic-guild-441706-k8", help="Project ID"
    )
    
    parser.add_argument(
        "--region", default="us-central1", help="region"
    )
    
    parser.add_argument(
        "--setup_file",
        default=None,
        help="Path to the setup.py file for packaging dependencies",
    )
    
    opts = parser.parse_args()
    PARENT_DIR = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    
    bigquery_schema = load_bigquery_schema(PARENT_DIR)

    options = PipelineOptions(
        runner=opts.runner,  # Use DataflowRunner to run on Google Cloud Dataflow
        project=opts.project,  # Replace with your GCP project ID
        temp_location=opts.temp_location,  # Replace with your GCS bucket for temp files
        region=opts.region,  # Region for Dataflow
        job_name="bigquery-dataflow-pipeline",  # Optional job name
        staging_location=opts.staging_location,
        setup_file=opts.setup_file,
        save_main_session=True,
    )
    options.view_as(StandardOptions).runner = opts.runner
    # options.view_as(GoogleCloudOptions).service_account_key_file = "/home/lokesh/Access-Token/kinetic-guild-441706-k8-fc93fcd454ae.json"

    p = beam.Pipeline(options=options)
    (
        p
        | "Read from files"
        >> beam.io.ReadFromText(
            "gs://webapp-logs-2024/raw_data/*.out"
        )
        # | "Read from files"
        # >> beam.io.ReadFromText(
        #     file_pattern=os.path.join(os.path.join(PARENT_DIR, "inputs", "*.out"))
        # )
        | "Parse elements" >> beam.Map(parse_json).with_output_types(EventLog)
        | "Add event timestamp" >> beam.Map(add_timestamp)
        | "Tumble window per minute" >> beam.WindowInto(beam.window.FixedWindows(60))
        | "Count per minute"
        >> beam.CombineGlobally(CountCombineFn()).without_defaults()
        | "Add window timestamp" >> beam.ParDo(AddWindowTS())
        | "Format for BigQuery" >> beam.ParDo(FormatForBigQuery())
        | "Write to Bigquery" >> beam.io.WriteToBigQuery(
            "kinetic-guild-441706-k8:webapp_logs_2024.page_view_counts",
            schema=bigquery_schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            additional_bq_parameters=additional_bq_parameters
        )

    )

    logging.getLogger().setLevel(logging.INFO)
    logging.info("Building pipeline ...")

    p.run().wait_until_finish()


if __name__ == "__main__":
    run()
