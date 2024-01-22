# -*- coding: utf-8 -*-


"""An Apache Beam batch pipeline example.
It reads JSON encoded messages from GCS file, transforms the message data and
writes the results to BigQuery.
"""

import argparse
import json
import logging
# import time
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

# from apache_beam.options.pipeline_options import SetupOptions
# import apache_beam.transforms.window as window

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
        'AIRLINE_NAME:STRING',
])
ERROR_SCHEMA = ','.join([
    'error:STRING',
])

class ParseMessage(beam.DoFn):
    OUTPUT_ERROR_TAG = 'error'

    def process(self, element, side_input,table):
        """
        Extracts fields from csv message
        :param element: line of file read from input gcs location.
        :return: have two outputs:
            - parsed data: parsed flights data with airline information
            - error data: error data
        """
        try:
            col_list = ['YEAR','MONTH','DAY','DAY_OF_WEEK','AIRLINE','FLIGHT_NUMBER','TAIL_NUMBER','ORIGIN_AIRPORT','DESTINATION_AIRPORT','SCHEDULED_DEPARTURE','DEPARTURE_TIME','DEPARTURE_DELAY','TAXI_OUT','WHEELS_OFF','SCHEDULED_TIME','ELAPSED_TIME','AIR_TIME','DISTANCE','WHEELS_ON','TAXI_IN','SCHEDULED_ARRIVAL','ARRIVAL_TIME','ARRIVAL_DELAY','DIVERTED','CANCELLED','CANCELLATION_REASON','AIR_SYSTEM_DELAY','SECURITY_DELAY','AIRLINE_DELAY','LATE_AIRCRAFT_DELAY','WEATHER_DELAY']
            #row = json.dumps(line)
            csv_element_list = element.replace('\n','').split(',') # parse csv line to corresponding bgiquery table schema
            final_dict = dict(zip(col_list,csv_element_list))
            logging.info("Running")
            print(side_input)
            final_dict['AIRLINE_NAME'] = side_input[csv_element_list[4]]
            yield final_dict

        except Exception as error:
            #print("error")
            logging.info("error")
            print(str(error))
            error_row = { 'error': str(error) }
            yield beam.pvalue.TaggedOutput(self.OUTPUT_ERROR_TAG, error_row)


class DataflowOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument(
            '--input_path', type=str, default='gs://dezyre-test-bucket1/data/test.csv',
            help='Input GCS path from where files will be read.')
        parser.add_value_provider_argument(
            '--side_input_path', type=str, default='gs://dezyre-test-bucket1/data/airlines.csv',
            help='Input GCS path from where side input files will be read.')
        parser.add_value_provider_argument(
            '--table', type=str, default='dataset1.demo1',
            help='Output BigQuery table for file specified as: PROJECT:DATASET.TABLE or DATASET.TABLE.')
        parser.add_value_provider_argument(
            '--error_table', type=str, default='dataset1.error',
            help='Output BigQuery table for error as: PROJECT:DATASET.TABLE or DATASET.TABLE.')


def run(argv=None):
    """Build and run the pipeline."""
    parser = argparse.ArgumentParser(argv)
    known_args, pipeline_args = parser.parse_known_args()
    options = PipelineOptions(pipeline_args, save_main_session=True)
    dataflow_options = options.view_as(DataflowOptions)
    with beam.Pipeline(options=options) as pipeline:
        print(str(dataflow_options.side_input_path))
        print("dome")
        side_input_airline = pipeline | 'Read airline messages' >> beam.io.ReadFromText(dataflow_options.side_input_path) \
                                      | beam.Map(lambda record: ('%s' % record.split(',')[0], '%s' % record.split(',')[1]))

                                      #| beam.Map(lambda record: {record.split(',')[0]:record.split(',')[1]})
        rows, error = (
                pipeline
                | 'Read CSV messages' >> beam.io.ReadFromText(dataflow_options.input_path,skip_header_lines=1)
                | 'Parse CSV messages' >> beam.ParDo(ParseMessage(), beam.pvalue.AsDict(side_input_airline),dataflow_options.table).with_outputs(
            ParseMessage.OUTPUT_ERROR_TAG,
            main='rows')
        )

        # Output the results into BigQuery table.
        _ = (rows | 'Write rows to BigQuery'
             >> beam.io.WriteToBigQuery(table=dataflow_options.table,
                                        schema=schema,
                                        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                                        # insert_retry_strategy=RetryStrategy.RETRY_ON_TRANSIENT_ERROR
                                        )
             )

        _ = (error | 'Write error to BigQuery'
             >> beam.io.WriteToBigQuery(table=dataflow_options.error_table,
                                        schema=ERROR_SCHEMA,
                                        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                                        # insert_retry_strategy=RetryStrategy.RETRY_ON_TRANSIENT_ERROR
                                        )
             )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
