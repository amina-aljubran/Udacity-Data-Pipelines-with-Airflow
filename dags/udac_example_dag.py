from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries


# default_args details
default_args = {
    'owner': 'udacity',
    'redshift_conn_id' : 'redshift',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2019, 1, 12),
    'email_on_retry': False
}

with DAG('udac_example_dag', default_args=default_args,
          description='Load and transform data', schedule_interval='@hourly'
        ) as dag:

    # first operator "Node"
    start_operator = DummyOperator(task_id='Begin_execution')

    # stage_events_to_redshift details
    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        table="staging_events",
        aws_credentials_id="aws_credentials",
        s3_bucket="udacity-dend",
        s3_key="log_data",
        json_path="s3://udacity-dend/log_json_path.json",
        file_type="json"
    )

    # stage_songs_to_redshift details
    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        table="staging_songs", 
        aws_credentials_id="aws_credentials",
        s3_bucket="udacity-dend",
        s3_key="song_data/A/A/A",
        json_path="auto",
        file_type="json"
    )

    # load_songplays_table details
    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        table='songplays',
        load_sql_stmt=SqlQueries.songplay_table_insert
    )

    # load_user_dimension_table details
    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        table='users',
        load_sql_stmt=SqlQueries.user_table_insert
    )

    # load_song_dimension_table details
    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        table='songs',
        load_sql_stmt=SqlQueries.song_table_insert
    )

    # load_artist_dimension_table details
    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        table='artists',
        load_sql_stmt=SqlQueries.artist_table_insert
    )

    # load_time_dimension_table details
    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        table='time',
        load_sql_stmt=SqlQueries.time_table_insert
    )

    # run_quality_checks details
    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        tables=['songplays', 'users', 'songs', 'artists', 'time'],
    )

    # Last operator details
    end_operator = DummyOperator(task_id='Stop_execution')

    # Task Dependencies
    # start nodes from start_operator Then [stage_events_to_redshift, stage_songs_to_redshift] Then
    # "load_songplays_table" is connected to 4 nodes that connected to 
    # "run_quality_checks" Then "end_operator" 
    start_operator >> [stage_events_to_redshift, stage_songs_to_redshift] >> [load_song_dimension_table, load_user_dimension_table, load_artist_dimension_table,  load_time_dimension_table] >> run_quality_checks >> end_operator 
