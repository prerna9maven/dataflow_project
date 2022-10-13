import argparse
import logging
import re
from apache_beam.io.gcp.bigquery import parse_table_schema_from_json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import json

PROJECT_ID = "df-project-356804"
#SCHEMA = parse_table_schema_from_json(json.dumps(json.load(open("emp_proj/schema.json"))))
SCHEMA = parse_table_schema_from_json(json.dumps(json.load(open("gs://us-central1-projemp-c0415a5e-bucket/dags/schema.json"))))

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
                        default='gs://projemployeebucket/100 Records.csv')
    # Parse arguments from the command line.
    known_args, pipeline_args = parser.parse_known_args(argv)
    

    beam_options = PipelineOptions(
    pipeline_args,
    runner='DataflowRunner',
    project='df-project-356804',
    job_name='dataloadjob',
    temp_location='gs://projemployeebucket/temp',
    staging_location='gs://projemployeebucket/temp',
    input= "gs://projemployeebucket/100 Records.csv",
    region='us-central1'
    )
    
    p = beam.Pipeline(options=beam_options)
    #p = beam.Pipeline(options=PipelineOptions(pipeline_args))



    
    data_ingestion = DataIngestion()
    
    
    (p
     | 'Read from a File' >> beam.io.ReadFromText(known_args.input, skip_header_lines=1)
     | 'String To BigQuery Row' >> beam.Map(lambda s: data_ingestion.parse_method(s))
     | 'Write to BigQuery' >> beam.io.Write(beam.io.BigQuerySink('{0}:employee.employee_temp'.format(PROJECT_ID),
                                            schema=SCHEMA,
                                            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)))     
    p.run().wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()