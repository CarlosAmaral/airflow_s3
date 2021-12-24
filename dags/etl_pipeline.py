from datetime import datetime, timedelta
import os
from airflow import conf
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

default_args = {
    'owner': 'carlos',
    'aws_hook': AwsHook('aws_credentials'),
    'start_date': datetime(2021, 12, 23),
    'schedule_interval': '@hourly',
    'depends_on_past': False,
    'catchup': False,
    'retry_delay': timedelta(minutes=5),
    'retries': 3,
    'email_on_retry': False
}

dag = DAG('etl_pipeline',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow', 
        )

create_tables_file = open(os.path.join(conf.get('core','dags_folder'),'create_tables.sql'))
create_tables_sql = create_tables_file.read()

create_tables = PostgresOperator(
    task_id="Create_tables",
    dag=dag,
    postgres_conn_id="redshift",
    sql=create_tables_sql
)

start_operator = DummyOperator(task_id='Start_pipeline',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table_name="staging_events",
    aws_credentials_id="aws_credentials",
    s3_bucket_name="udacity-dend",
    s3_bucket_key="log_data",
    extra_params="format as json 's3://udacity-dend/log_json_path.json'",
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table_name="staging_songs",
    aws_credentials_id="aws_credentials",
    s3_bucket_name="udacity-dend",
    s3_bucket_key="song_data",
    extra_params="json 'auto' compupdate off",
)
  
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    table_name="songplays",
    sql_content=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    table_name="users",
    sql_content=SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    table_name="songs",
    sql_content=SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    table_name="artists",
    sql_content=SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table_name="time",
    sql_content=SqlQueries.time_table_insert
   
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    dq_checks=[
        {'check_sql': "SELECT COUNT(*) FROM users WHERE userid is null", 'expected_result': 0},
        {'check_sql': "SELECT COUNT(*) FROM songs WHERE song_id is null", 'expected_result': 0},
        {'check_sql': "SELECT COUNT(*) FROM artists WHERE artist_id is null", 'expected_result': 0}
    ]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

create_tables >> start_operator
start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator
