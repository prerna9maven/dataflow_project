
import os
from re import template
from airflow import DAG
import apache_beam as beam
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
from google.cloud import bigquery
from airflow import models
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowConfiguration
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator


default_dag_args = {
# Setting start date as today 
'start_date': datetime.today(),
# To email on failure or retry set 'email' arg to your email and enable
# emailing here.
'email_on_failure': False,
'email_on_retry': False,
# If a task fails, retries can be mentioned here
'retries': 0,
#specifying the project id
'project_id': 'df-project-356804'
} 

with DAG("df-employee-dag",schedule_interval='@once',default_args=default_dag_args, catchup=False) as dag:

    start = DummyOperator(task_id='start'),
    end = DummyOperator(task_id='end'),


    start_python_job_dataload = BeamRunPythonPipelineOperator(
    task_id="start_python_job_dataload",
    runner="DataflowRunner",
    py_file="gs://us-central1-projemp-c0415a5e-bucket/dags/Data_load_pipeline_for_dag.py",
    py_options=[],
    pipeline_options={'tempLocation': 'gs://us-central1-projemp-c0415a5e-bucket/dags/test', 
                      'stagingLocation': 'gs://us-central1-projemp-c0415a5e-bucket/dags/test'},
    py_requirements=['apache-beam[gcp]==2.25.0'],
    py_interpreter='python3',
    py_system_site_packages=False,
    dataflow_config={
        "job_name": "start_python_job_dataload",
        "location": 'us-central1',
        "wait_until_finished": False,
        }
    )


    start_python_job_transform = BeamRunPythonPipelineOperator(
    task_id="start_python_job_transform",
    runner="DataflowRunner",
    py_file="gs://us-central1-projemp-a6e6e0a5-bucket/dags/Data_transform_pipeline_for_dag.py",
    py_options=[],
    pipeline_options={'tempLocation': 'gs://us-central1-projemp-a6e6e0a5-bucket/dags/test', 
                      'stagingLocation': 'gs://us-central1-projemp-a6e6e0a5-bucket/dags/test'},
    py_requirements=['apache-beam[gcp]==2.25.0'],
    py_interpreter='python3',
    py_system_site_packages=False,
    dataflow_config={
        "job_name": "start_python_job_transform",
        "location": 'us-central1',
        "wait_until_finished": False,
        }
    )


    start >> start_python_job_dataload >> start_python_job_transform >> end