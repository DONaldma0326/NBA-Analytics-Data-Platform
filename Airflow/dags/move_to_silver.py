from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator  

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),

}
# DAG definition
dag = DAG(
    'Bronze_to_silver',
    default_args=default_args,
    description='Move data from Bronze to Silver layer using AWS Glue',
    catchup=False,
    tags=['bronze', 'silver', 'nba', 'project'],
)



# Task to trigger AWS Glue job using AwsGlueJobOperator directly
trigger_glue = GlueJobOperator(
    task_id='bronze_to_silver_job',
    aws_conn_id = 'Glue',
    job_name='player_total',  # Replace with your Glue job name
    script_location='s3://aws-glue-assets-520551197923-ap-southeast-1/scripts/player_total.py',  # Replace with your script location
    region_name='ap-southeast-1',
    script_args={
        '--etl_date': datetime.now().strftime('%Y-%m')
    },
    wait_for_completion=True,  # Wait for the Glue job to finish and listen to its result
    dag=dag,
)
