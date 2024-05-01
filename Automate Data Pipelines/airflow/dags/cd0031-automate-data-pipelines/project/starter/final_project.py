import os
import pendulum

from airflow.decorators import dag
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.data_quality import DataQualityOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from udacity.common.final_project_sql_statements import SqlQueries

default_args = {
    'owner': 'Jacob Celestine',
    'start_date': datetime(2024, 5, 1),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'catchup': False
}

REGION = 'us-east-1'
REDSHIFT_CONN_ID = "redshift"
AWS_CREDENTIALS_ID = "aws_credentials"
S3_SONG_KEY = 'song-data'
S3_BUCKET = 'sean-murdock-1'
S3_LOG_KEY = 'log-data/{execution_date.year}/{execution_date.month}'
LOG_JSON_PATH = f's3://{S3_BUCKET}/log_json_path.json'


@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *',

)
def final_project():

    start_operator = DummyOperator(task_id='Begin_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        redshift_conn_id=REDSHIFT_CONN_ID,
        aws_conn_id=AWS_CREDENTIALS_ID,
        table_name='staging_events',
        s3_bucket=S3_BUCKET,
        s3_key=S3_LOG_KEY,
        region=REGION,
        truncate_table=True,
        data_format=f"JSON '{LOG_JSON_PATH}'"
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        redshift_conn_id=REDSHIFT_CONN_ID,
        aws_conn_id=AWS_CREDENTIALS_ID,
        table_name='staging_songs',
        s3_bucket=S3_BUCKET,
        s3_key=S3_SONG_KEY,
        region=REGION,
        truncate_table=True,
        data_format="JSON 'auto'"
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        conn_id=REDSHIFT_CONN_ID,
        query=SqlQueries.songplay_table_insert,
        table_name='songplays'
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        conn_id = REDSHIFT_CONN_ID,
        query = SqlQueries.user_table_insert,
        table_name = 'users',
        mode = "truncate"
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        conn_id = REDSHIFT_CONN_ID,
        query = SqlQueries.song_table_insert,
        table_name = 'songs',
        mode = "truncate"
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        conn_id = REDSHIFT_CONN_ID,
        query = SqlQueries.artist_table_insert,
        table_name = 'artists',
        mode = "truncate"
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        conn_id = REDSHIFT_CONN_ID,
        query = SqlQueries.time_table_insert,
        table_name = 'time',
        mode = "truncate"
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        conn_id= REDSHIFT_CONN_ID,
        test_case_queries = [{"sql_statement":"SELECT count(*) FROM songplays WHERE playid is null", "expected_result":0}]
    )

    end_operator = DummyOperator(task_id='End_execution')

    start_operator >> stage_events_to_redshift >> load_songplays_table
    start_operator >> stage_songs_to_redshift >> load_songplays_table
    load_songplays_table >> load_song_dimension_table >> run_quality_checks
    load_songplays_table >> load_user_dimension_table >> run_quality_checks
    load_songplays_table >> load_artist_dimension_table >> run_quality_checks
    load_songplays_table >> load_time_dimension_table >> run_quality_checks
    run_quality_checks >> end_operator

final_project_dag = final_project()
