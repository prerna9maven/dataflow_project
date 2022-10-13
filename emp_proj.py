import argparse
import logging
import re
from apache_beam.io.gcp.bigquery import parse_table_schema_from_json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import json
from datetime import datetime

SCHEMA = parse_table_schema_from_json(json.dumps(json.load(open("mini-project/final/schema.json"))))



def date_cols(data):
    etl_date= datetime.datetime.now()
    n_row = {'created_time': etl_date.strftime('%Y-%m-%d %H:%M:%S'),
            'modified_time': etl_date.strftime('%Y-%m-%d %H:%M:%S')}


class DataIngestion:
    def parse_method(self, string_input):
        values = re.split(",", re.sub('\r\n', '', re.sub('"', '', string_input)))
        row = dict(
            zip(("Emp_ID",	"Name_Prefix",	"First_Name", "Middle_Initial",	"Last_Name",	
                "Gender",	"E_Mail",	"Father_s_Name",  "Mother_s_Name",  "Mother_s_Maiden_Name",
                "Date_of_Birth","Time_of_Birth","Age_in_Yrs_","Weight_in_Kgs_","Date_of_Joining",
                "Quarter_of_Joining","Half_of_Joining","Year_of_Joining","Month_of_Joining","Month_Name_of_Joining",
                "Short_Month","Day_of_Joining","DOW_of_Joining","Short_DOW","Age_in_Company__Years_",
                "Salary","Last___Hike","SSN","Phone_No_","Place_Name", "County","City","State",
                "Zip","Region","User_Name","Password"), values))
        return row


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--input',
                        dest='input',
                        required=False,
                        help='Input file to read. This can be a local file or '
                        'a file in a Google Storage Bucket.',
                        default='gs://mini-project-mw/100 Records.csv')
    parser.add_argument('--output',
                        dest='output',
                        required=False,
                        help='Output BQ table to write results to.',
                        default='Employee.employee_details')

    # Parse arguments from the command line.
    known_args, pipeline_args = parser.parse_known_args(argv)

    data_ingestion = DataIngestion()
    p = beam.Pipeline(options=PipelineOptions(pipeline_args))

    (p
     | 'Read from a File' >> beam.io.ReadFromText(known_args.input, skip_header_lines=1)
     | 'String To BigQuery Row' >> beam.Map(lambda s: data_ingestion.parse_method(s))
     | 'Add new columns' >> beam.Map(date_cols(data))
     | 'Write to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(known_args.output,
                                            schema=SCHEMA,
                                            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)))
    p.run().wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()  