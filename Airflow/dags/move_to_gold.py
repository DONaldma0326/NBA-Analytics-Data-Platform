import os
import time
from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

try:
    GLUE_JOB_WAIT_SECONDS = int(os.getenv('GLUE_JOB_WAIT_SECONDS', '60'))
except ValueError:
    GLUE_JOB_WAIT_SECONDS = 60

with DAG(
    'move_to_gold',
    default_args=default_args,
    description='Move data from Silver to Gold layer using AWS Glue',
    catchup=False,
    tags=['silver', 'gold', 'nba', 'project'],
) as dag:

    @task
    def get_latest_player_silver_parquet(bucket, prefix):
        s3_hook = S3Hook(aws_conn_id='S3')
        files = s3_hook.list_keys(bucket_name=bucket, prefix=prefix)
        parquet_files = [
            key for key in files
            if key.endswith('.parquet') 
        ]
        print(f"Found parquet files: {parquet_files}")
        if not parquet_files:
            raise ValueError("No parquet files found for the current year and month in the specified prefix.")
        latest_file = max(
            parquet_files,
            key=lambda key: s3_hook.get_key(key, bucket_name=bucket).last_modified
        )
        print(f"Latest Silver Parquet file: {latest_file}")
        return f"s3://{bucket}/{latest_file}"
    
    @task
    def get_latest_team_silver_parquet(bucket, prefix):
        s3_hook = S3Hook(aws_conn_id='S3')
        files = s3_hook.list_keys(bucket_name=bucket, prefix=prefix)
        parquet_files = [
            key for key in files
            if key.endswith('.parquet') 
        ]
        print(f"Found parquet files: {parquet_files}")
        if not parquet_files:
            raise ValueError("No parquet files found for the current year and month in the specified prefix.")
        latest_file = max(
            parquet_files,
            key=lambda key: s3_hook.get_key(key, bucket_name=bucket).last_modified
        )
        print(f"Latest Silver Parquet file: {latest_file}")
        return f"s3://{bucket}/{latest_file}"

    @task
    def wait_between_glue_jobs(job_label: str, wait_seconds: int = GLUE_JOB_WAIT_SECONDS):
        print(f"Waiting {wait_seconds} seconds after Glue job {job_label}.")
        time.sleep(wait_seconds)

    latest_player_silver_parquet = get_latest_player_silver_parquet(
        bucket='nbaanalysisproject',
        prefix=f"silver/ingest_date={datetime.now().strftime('%Y-%m')}/playerdata/"
    )

    latest_team_silver_parquet = get_latest_team_silver_parquet(
        bucket='nbaanalysisproject',
        prefix=f"silver/ingest_date={datetime.now().strftime('%Y-%m')}/teamdata/"
    )

    current_year = datetime.now().year
    previous_player_task = None
    previous_team_task = None

    for season in range(2016, current_year + 1):
        trigger_glue = GlueJobOperator(
            task_id=f'silver_to_gold_job_{season}',
            aws_conn_id='Glue',
            job_name='SilverToGold_Player',
            script_location='s3://aws-glue-assets-520551197923-ap-southeast-1/scripts/SilverToGold_Player.py',
            region_name='ap-southeast-1',
            script_args={
                '--season': str(season),
                '--silver_path': "{{ ti.xcom_pull(task_ids='get_latest_player_silver_parquet') }}",
                '--gold_path': 's3://nbaanalysisproject/gold/playerdata/',
            },
            wait_for_completion=True,
            deferrable=False,  # Ensures Airflow waits for job completion and fails on error
        )
        latest_player_silver_parquet >> trigger_glue
        if previous_player_task:
            previous_player_task >> trigger_glue
        wait_player = wait_between_glue_jobs.override(task_id=f'wait_after_player_glue_{season}')(
            job_label=f'player_{season}',
            wait_seconds=GLUE_JOB_WAIT_SECONDS,
        )
        trigger_glue >> wait_player
        previous_player_task = wait_player
    


    for season in range(2016, current_year + 1):
        trigger_glue_team = GlueJobOperator(
            task_id=f'silver_to_gold_team_job_{season}',
            aws_conn_id='Glue',
            job_name='SilverToGold_Team',
            script_location='s3://aws-glue-assets-520551197923-ap-southeast-1/scripts/SilverToGold_Team.py',
            region_name='ap-southeast-1',
            script_args={
                '--season': str(season),
                '--silver_path': "{{ ti.xcom_pull(task_ids='get_latest_team_silver_parquet') }}",
                '--gold_path': 's3://nbaanalysisproject/gold/teamdata/',
            },
            wait_for_completion=True,
            deferrable=False,
        )
        latest_team_silver_parquet >> trigger_glue_team
        if previous_team_task:
            previous_team_task >> trigger_glue_team
        wait_team = wait_between_glue_jobs.override(task_id=f'wait_after_team_glue_{season}')(
            job_label=f'team_{season}',
            wait_seconds=GLUE_JOB_WAIT_SECONDS,
        )
        trigger_glue_team >> wait_team
        previous_team_task = wait_team


        