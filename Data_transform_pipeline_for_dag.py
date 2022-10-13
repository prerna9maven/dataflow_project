import argparse
import logging
import re
from apache_beam.io.gcp.bigquery import parse_table_schema_from_json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import json

PROJECT_ID = "df-project-356804"
#SCHEMA1 = parse_table_schema_from_json(json.dumps(json.load(open("emp_proj/schema1.json"))))
SCHEMA1 = parse_table_schema_from_json(json.dumps(
                {
                "fields": [{"name": "Emp_ID","type": "INTEGER","mode": "NULLABLE"},
                           {"name": "Name_Prefix","type": "STRING","mode": "NULLABLE"},
                           {"name": "First_Name","type": "STRING","mode": "NULLABLE"},
                           {"name": "Middle_Initial","type": "STRING","mode": "NULLABLE"},
                           {"name": "Last_Name","type": "STRING","mode": "NULLABLE"},
                           {"name": "Gender","type": "STRING","mode": "NULLABLE"},
                           {"name": "E_Mail","type": "STRING","mode": "NULLABLE"},
                           {"name": "Father_s_Name","type": "STRING","mode": "NULLABLE"},
                           {"name": "Mother_s_Name","type": "STRING","mode": "NULLABLE"},
                           {"name": "Mother_s_Maiden_Name","type": "STRING","mode": "NULLABLE"},
                           {"name": "Date_of_Birth","type": "STRING","mode": "NULLABLE"},
                           {"name": "Time_of_Birth","type": "STRING","mode": "NULLABLE"},
                           {"name": "Age_in_Yrs_","type": "FLOAT","mode": "NULLABLE"},
                           {"name": "Weight_in_Kgs_","type": "INTEGER","mode": "NULLABLE"},
                           {"name": "Date_of_Joining","type": "STRING","mode": "NULLABLE"},
                           {"name": "Quarter_of_Joining","type": "STRING","mode": "NULLABLE"},
                           {"name": "Half_of_Joining","type": "STRING","mode": "NULLABLE"},
                           {"name": "Year_of_Joining","type": "INTEGER","mode": "NULLABLE"},
                           {"name": "Month_of_Joining","type": "INTEGER","mode": "NULLABLE"},
                           {"name": "Month_Name_of_Joining","type": "STRING","mode": "NULLABLE"},
                           {"name": "Short_Month","type": "STRING","mode": "NULLABLE"},
                           {"name": "Day_of_Joining","type": "INTEGER","mode": "NULLABLE"},
                           {"name": "DOW_of_Joining","type": "STRING","mode": "NULLABLE"},
                           {"name": "Short_DOW","type": "STRING","mode": "NULLABLE"},
                           {"name": "Age_in_Company__Years_","type": "FLOAT","mode": "NULLABLE"},
                           {"name": "Salary","type": "INTEGER","mode": "NULLABLE"},
                           {"name": "Last___Hike","type": "STRING","mode": "NULLABLE"},
                           {"name": "SSN","type": "STRING","mode": "NULLABLE"},
                           {"name": "Phone_No_","type": "STRING","mode": "NULLABLE"},
                           {"name": "Place_Name","type": "STRING","mode": "NULLABLE"},
                           {"name": "County","type": "STRING","mode": "NULLABLE"},
                           {"name": "City","type": "STRING","mode": "NULLABLE"},
                           {"name": "State","type": "STRING","mode": "NULLABLE"},
                           {"name": "Zip","type": "INTEGER","mode": "NULLABLE"},
                           {"name": "Region","type": "STRING","mode": "NULLABLE"},
                           {"name": "User_Name","type": "STRING","mode": "NULLABLE"},
                           {"name": "Password","type": "STRING","mode": "NULLABLE"},
                           {"name": "Created_time","type": "DATETIME","mode": "NULLABLE"},
                           {"name": "Modified_time","type": "DATETIME","mode": "NULLABLE"}
                        ]
                } 
        ))



#SCHEMA2 = parse_table_schema_from_json(json.dumps(json.load(open("emp_proj/schema2.json"))))
SCHEMA2 = parse_table_schema_from_json(json.dumps(
            {
                "fields":  [{"name": "Emp_ID","type": "INTEGER","mode": "NULLABLE"},
                            {"name": "Name","type": "STRING","mode": "NULLABLE"},
                            {"name": "Gender","type": "STRING","mode": "NULLABLE"},
                            {"name": "EMail","type": "STRING","mode": "NULLABLE"},
                            {"name": "Date_of_Birth","type": "STRING","mode": "NULLABLE"},
                            {"name": "Date_of_Joining","type": "STRING","mode": "NULLABLE"},
                            {"name": "Age_in_Company","type": "FLOAT","mode": "NULLABLE"},
                            {"name": "Salary","type": "INTEGER","mode": "NULLABLE"},
                            {"name": "Created_Time","type": "DATETIME","mode": "NULLABLE"},
                            {"name": "Modified_Time","type": "DATETIME","mode": "NULLABLE"}
                            ]
            }                       
        ))



#SCHEMA3 = parse_table_schema_from_json(json.dumps(json.load(open("emp_proj/schema3.json"))))
SCHEMA3 = parse_table_schema_from_json(json.dumps(
                {
                "fields":  [{"name": "ID","type": "STRING","mode": "NULLABLE"},
                            {"name": "Emp_ID","type": "INTEGER","mode": "NULLABLE"},
                            {"name": "Father_Name","type": "STRING","mode": "NULLABLE"},
                            {"name": "Mother_Name","type": "STRING","mode": "NULLABLE"},
                            {"name": "Age_in_Yrs","type": "INTEGER","mode": "NULLABLE"},
                            {"name": "Weight_in_Kgs","type": "INTEGER","mode": "NULLABLE"},
                            {"name": "Phone_No","type": "STRING","mode": "NULLABLE"},
                            {"name": "State","type": "STRING","mode": "NULLABLE"},
                            {"name": "Zip","type": "INTEGER","mode": "NULLABLE"},
                            {"name": "Region","type": "STRING","mode": "NULLABLE"},
                            {"name": "Created_Time","type": "DATETIME","mode": "NULLABLE"},
                            {"name": "Modified_Time","type": "DATETIME","mode": "NULLABLE"}
                            ]
                } 
        ))

def run(argv=None):
    parser = argparse.ArgumentParser()
    
    known_args,pipeline_args = parser.parse_known_args(argv)
    
    
    query1 = """SELECT
                    *,
                    FORMAT_DATETIME('%Y-%m-%d %H:%M:%S', CURRENT_DATETIME()) AS Created_Time,
                    FORMAT_DATETIME('%Y-%m-%d %H:%M:%S', CURRENT_DATETIME()) AS Modified_Time
                FROM
                    `df-project-356804.employee.employee_temp`"""



    query2 = """SELECT
                    Emp_ID,
                    CONCAT(First_Name," ",Last_Name) AS Name,
                    Gender,
                    E_Mail AS Email,
                    Date_of_Birth,
                    Date_of_Joining,
                    Age_in_Company__Years_ AS Age_in_Company,
                    Salary,
                    FORMAT_DATETIME('%Y-%m-%d %H:%M:%S', CURRENT_DATETIME()) AS Created_Time,
                    FORMAT_DATETIME('%Y-%m-%d %H:%M:%S', CURRENT_DATETIME()) AS Modified_Time
                    FROM
                    `df-project-356804.employee.employee_temp`"""

    query3 = """SELECT
                    GENERATE_UUID() AS Id,
                    Emp_ID,
                    Father_s_Name AS Father_Name,
                    Mother_s_Name AS Mother_Name,
                    DATE_DIFF(CURRENT_DATE, COALESCE(SAFE.PARSE_DATE('%Y-%m-%d', Date_of_Birth),SAFE.PARSE_DATE('%d/%m/%Y', Date_of_Birth),SAFE.PARSE_DATE('%m/%d/%Y', Date_of_Birth)),year) AS Age_in_yrs,
                    Weight_in_Kgs_ AS Weight_in_kgs,
                    Phone_No_ AS Phone_no,
                    State,
                    Zip,
                    Region,
                    FORMAT_DATETIME('%Y-%m-%d %H:%M:%S', CURRENT_DATETIME()) AS Created_Time,
                    FORMAT_DATETIME('%Y-%m-%d %H:%M:%S', CURRENT_DATETIME()) AS Modified_Time
                    FROM
                    `df-project-356804.employee.employee_details_temp`"""

    
    beam_options = PipelineOptions(
    pipeline_args,
    runner='DataflowRunner',
    project='df-project-356804',
    job_name='datatransformjob',
    temp_location='gs://us-central1-projemp-c0415a5e-bucket/dags/test',
    staging_location='gs://us-central1-projemp-c0415a5e-bucket/dags/test',
    region='us-central1'
    )
    
    p = beam.Pipeline(options=beam_options)
    #p = beam.Pipeline(options=PipelineOptions(pipeline_args))

    (
     p                                    
     | 'Read from BigQuery1' >> beam.io.ReadFromBigQuery(query=query1, use_standard_sql=True)
     | 'Write to BigQuery1' >> beam.io.WriteToBigQuery('{0}:employee.employee_details_temp'.format(PROJECT_ID),
                                                schema=SCHEMA1,
                                                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))                                           



    
    (
     p                                    
     | 'Read from BigQuery2' >> beam.io.ReadFromBigQuery(query=query2, use_standard_sql=True)
     | 'Write to BigQuery2' >> beam.io.WriteToBigQuery('{0}:employee.employee_details2_1'.format(PROJECT_ID),
                                                schema=SCHEMA2,
                                                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))                                            



    (
     p                                    
     | 'Read from BigQuery3' >> beam.io.ReadFromBigQuery(query=query3, use_standard_sql=True)
     | 'Write to BigQuery3' >> beam.io.WriteToBigQuery('{0}:employee.employee_details2_2'.format(PROJECT_ID),
                                                schema=SCHEMA3,
                                                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
    
    
    
    p.run().wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()