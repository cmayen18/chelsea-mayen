# -*- coding: utf-8 -*-

"""An Apache Beam streaming pipeline example.
It reads JSON encoded messages from Pub/Sub, transforms the message data and
writes the results to BigQuery.
"""

import argparse
import json,ast
import logging
#import time
from apache_beam.io.gcp.bigquery_tools import RetryStrategy
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import bigquery
#from apache_beam.options.pipeline_options import SetupOptions
#import apache_beam.transforms.window as window

# Defines the BigQuery schema for the output table.




schema=','.join([
        'YEAR:INTEGER',
        'MONTH:INTEGER',
        'DAY:INTEGER',
        'DAY_OF_WEEK:INTEGER',
        'AIRLINE:STRING',
        'FLIGHT_NUMBER:INTEGER',
        'TAIL_NUMBER:STRING',
        'ORIGIN_AIRPORT:STRING',
        'DESTINATION_AIRPORT:STRING',
        'SCHEDULED_DEPARTURE:INTEGER',
        'DEPARTURE_TIME:INTEGER',
        'DEPARTURE_DELAY:INTEGER',
        'TAXI_OUT:INTEGER',
        'WHEELS_OFF:INTEGER',
        'SCHEDULED_TIME:INTEGER',
        'ELAPSED_TIME:INTEGER',
        'AIR_TIME:INTEGER',
        'DISTANCE:INTEGER',
        'WHEELS_ON:INTEGER',
        'TAXI_IN:INTEGER',
        'SCHEDULED_ARRIVAL:INTEGER',
        'ARRIVAL_TIME:INTEGER',
        'ARRIVAL_DELAY:INTEGER',
        'DIVERTED:INTEGER',
        'CANCELLED:INTEGER',
        'CANCELLATION_REASON:STRING',
        'AIR_SYSTEM_DELAY:INTEGER',
        'SECURITY_DELAY:INTEGER',
        'AIRLINE_DELAY:INTEGER',
        'LATE_AIRCRAFT_DELAY:INTEGER',
        'WEATHER_DELAY:INTEGER',
])
ERROR_SCHEMA = ','.join([
    'error:STRING',
])


class ParseMessage(beam.DoFn):
    OUTPUT_ERROR_TAG = 'error'
    def process(self, line):
        """
        Extracts fields from json message
        :param line: pubsub message
        :return: have two outputs:
            - main: parsed data
            - error: error message
        """
        try:
            #row = json.dumps(line)
            parsed_row = ast.literal_eval(line) # parse json message to corresponding bgiquery table schema
            logging.info("Running")
            yield parsed_row

        except Exception as error:
            #print("error")
            logging.info("error")
            error_row = { 'error': str(error) }
            yield beam.pvalue.TaggedOutput(self.OUTPUT_ERROR_TAG, error_row)



def run(args, input_subscription, output_table, output_error_table):
    """Build and run the pipeline."""
    options = PipelineOptions(args, save_main_session=True, streaming=True)

    with beam.Pipeline(options=options) as pipeline:
        # Read the messages from PubSub and process them.
        rows, error_rows = (
            pipeline
            | 'Read from Pub/Sub' >> beam.io.ReadFromPubSub(
                subscription=input_subscription).with_output_types(bytes)
            | 'UTF-8 bytes to string' >> beam.Map(lambda msg: msg.decode('utf-8'))
            | 'Parse JSON messages' >> beam.ParDo(ParseMessage()).with_outputs(ParseMessage.OUTPUT_ERROR_TAG,
                                                                                main='rows')
             )
            #| 'Add URL keys' >> beam.Map(lambda msg: (msg['url'], msg))
            #| 'Group by URLs' >> beam.GroupByKey()
            #| 'Get statistics' >> beam.Map(get_statistics))

        # Output the results into BigQuery table.
        _ = (rows | 'Write to BigQuery'
             >> beam.io.WriteToBigQuery(output_table,
                                        schema=schema,
                                        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                                        insert_retry_strategy=RetryStrategy.RETRY_ON_TRANSIENT_ERROR
                                        )
             )

        _ = (error_rows | 'Write errors to BigQuery'
             >> beam.io.WriteToBigQuery(output_error_table,
                                        schema=ERROR_SCHEMA,
                                        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                                        insert_retry_strategy=RetryStrategy.RETRY_ON_TRANSIENT_ERROR
                                        )
             )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input_subscription', required=True,
        help='Input PubSub subscription of the form "/subscriptions/<PROJECT>/<SUBSCRIPTION>".')
    parser.add_argument(
        '--output_table', required=True,
        help='Output BigQuery table for results specified as: PROJECT:DATASET.TABLE or DATASET.TABLE.')
    parser.add_argument(
        '--output_error_table', required=True,
        help='Output BigQuery table for errors specified as: PROJECT:DATASET.TABLE or DATASET.TABLE.')
    known_args, pipeline_args = parser.parse_known_args()
    run(pipeline_args, known_args.input_subscription, known_args.output_table, known_args.output_error_table)

