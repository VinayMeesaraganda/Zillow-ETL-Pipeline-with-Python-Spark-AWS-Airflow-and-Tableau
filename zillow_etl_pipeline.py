#imports related to python
import logging
import requests
import time
from json import dumps
import boto3
import csv
from io import StringIO

#imports related to Airflow dags
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta,date
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.emr import (
    EmrCreateJobFlowOperator, 
    EmrAddStepsOperator,
    EmrTerminateJobFlowOperator)
from airflow.providers.amazon.aws.sensors.emr import EmrJobFlowSensor, EmrStepSensor

# API Configuration
api_url = "https://zillow56.p.rapidapi.com/search"
querystring = {"location":"fuquay-varina, nc"}
locations = ["Garner, nc","Raleigh, nc","Greensboro, nc","Charlotte, nc","Concord, nc","Fuquay Varina, nc","Durham, nc","Cary, nc","Holly Springs, nc","Fayetteville, nc"]
headers = {
	"X-RapidAPI-Key": "API_Key",
	"X-RapidAPI-Host": "zillow56.p.rapidapi.com"
}

# EMR Cluster Configuration
job_flow_overrides = {
    "Name": "zillow_emr_cluster",
    "ReleaseLabel": "emr-6.13.0",
    "Applications": [{"Name": "Spark"}, {"Name": "JupyterEnterpriseGateway"}],
    "LogUri": "s3://zillow-data-project-yml/emr-logs-yml/",
    "VisibleToAllUsers":False,
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master node",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Core node",
                "Market": "ON_DEMAND", # Spot instances are a "use as available" instances
                "InstanceRole": "CORE",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 2,
            },
        ],
         
        "Ec2SubnetId": "your_subnet_ID",
        "Ec2KeyName" : 'emr-keypair-airflow',
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False, # Setting this as false will allow us to programmatically terminate the cluster
    },
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
   
}

# Spark Transformation Steps
SPARK_STEPS_TRANSFORMATION = [
    {
        "Name": "Transform Redfin data",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": ["spark-submit",
                     "--packages", "org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk:1.12.571",
                     "s3://zillow-data-project-yml/scripts/zillow_etl_transformation.py"
            ],
        },
    },
   ]

# Function to get Zillow data from API
def get_zillow_data(api_url,locations):
    try:
        csv_buffer = StringIO()
        csv_writer = csv.writer(csv_buffer)

        # Write header row
        csv_writer.writerow(['bathrooms', 'bedrooms', 'city', 'country', 'currency',
                            'daysOnZillow', 'homeStatus', 'homeType', 'imgSrc', 'isFeatured',
                            'isNonOwnerOccupied', 'isPreforeclosureAuction', 'isPremierBuilder',
                            'isZillowOwned', 'latitude', 'longitude','livingArea', 'lotAreaValue',
                            'price', 'zestimate','rentZestimate', 'streetAddress',
                            'taxAssessedValue', 'zipcode', 'zpid'])
        for location in locations:
            querystring['location']=location
            response = requests.get(api_url, headers=headers, params=querystring)

            # Check if the request was successful (status code 200)
            if response.status_code == 200:
                # Convert the response content to JSON
                json_data = response.json()

                # Extract results array
                results = json_data.get('results', [])
                for result in results:
                    csv_writer.writerow([result.get('bathrooms'), result.get('bedrooms'), result.get('city'),
                                        result.get('country'), result.get('currency'), result.get('daysOnZillow'),
                                        result.get('homeStatus'), result.get('homeType'), result.get('imgSrc'),
                                        result.get('isFeatured'), result.get('isNonOwnerOccupied'),
                                        result.get('isPreforeclosureAuction'), result.get('isPremierBuilder'),
                                        result.get('isZillowOwned'), result.get('latitude'), result.get('longitude'),
                                        result.get('livingArea'),result.get('lotAreaValue'), result.get('price'), result.get('zestimate'),
                                        result.get('rentZestimate'), result.get('streetAddress'),
                                        result.get('taxAssessedValue'), result.get('zipcode'), result.get('zpid')])
            time.sleep(2)
        csv_content = csv_buffer.getvalue()
        return csv_content
    except requests.RequestException as e:
        logging.error("API request failed for %s: %s", api_url, e)
        return None
    
# Function to publish zillow property data to AWS S3
def publish_property_data_to_s3(**kwargs):
    api_data = kwargs['ti'].xcom_pull(task_ids='get_api_data')
    try:
        s3_bucket = "zilow-data-transformed"
        todays_date = date.today().strftime("%Y-%m-%d")
        s3_key = f"Api_data/zillow_data_{todays_date}.csv"
        # Create a Boto3 S3 client
        s3_client = boto3.client('s3')
        s3_client.put_object(Body=api_data, Bucket=s3_bucket, Key=s3_key)
    except Exception as e:
        logging.error('Error sending data to S3: %s', e)

# Airflow DAG Configuration
default_args = {
    'owner': 'Vinay',
    'start_date': datetime(2024, 1,7),
    'retries': 0,
    'schedule_interval': None,
    'catchup': False 
}

zillow_data_dag = DAG(
    dag_id='zillow_dag_api',
    default_args=default_args,
    description='get user data',
    catchup= False
)

# Airflow Operators
start_pipeline= EmptyOperator(
    task_id='start_pipeline'
)

get_zillow_property_data_api = PythonOperator(
    task_id='get_api_data', 
    python_callable=get_zillow_data,
    op_kwargs={'api_url':api_url,'locations': locations},
    dag=zillow_data_dag
)

publish_property_data_to_AWS_s3 = PythonOperator(
   task_id='publish_to_s3',
   python_callable=publish_property_data_to_s3,
   dag=zillow_data_dag
)

# Create an EMR cluster
create_emr_cluster = EmrCreateJobFlowOperator(
    task_id="create_emr_cluster",
    job_flow_overrides=job_flow_overrides,
    dag=zillow_data_dag
)

is_emr_cluster_created = EmrJobFlowSensor(
task_id="is_emr_cluster_created", 
job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
target_states={"WAITING"},  # Specify the desired state
timeout=3600,
poke_interval=5,
mode='poke',
)

spark_transformation_step = EmrAddStepsOperator(
task_id="execute_transformation_step",
job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
steps=SPARK_STEPS_TRANSFORMATION,
)

is_transformation_completed = EmrStepSensor(
task_id="is_transformation_completed",
job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
step_id="{{ task_instance.xcom_pull(task_ids='execute_transformation_step')[0] }}",
target_states={"COMPLETED"},
timeout=3600,
poke_interval=10,
)

remove_cluster = EmrTerminateJobFlowOperator(
task_id="remove_cluster",
job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
)

is_emr_cluster_terminated = EmrJobFlowSensor(
task_id="is_emr_cluster_terminated", 
job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
target_states={"TERMINATED"},  # Specify the desired state
timeout=3600,
poke_interval=5,
mode='poke',
)

# DAG Execution Flow
start_pipeline >> get_zillow_property_data_api >> publish_property_data_to_AWS_s3 >> create_emr_cluster >> is_emr_cluster_created \
>> spark_transformation_step >> is_transformation_completed >> remove_cluster >> is_emr_cluster_terminated
