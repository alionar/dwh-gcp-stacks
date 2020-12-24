from airflow import DAG
# from airflow.contrib.operators.bigquery_operator import BigQueryOperator
# from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator
from helper.bigquery_check_operator import BigQueryCheckOperator
from helper.bigquery_operator import BigQueryOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import os
from helper.gcs import *
from helper.bq import *
from helper.kaggle_download import *

default_args = {
    'owner': 'Aulia Lionar',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'start_date': datetime(2020,12,22),
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

# VARIABLE
airflow_home = '/usr/local/airflow'
project_id = 'certain-region-299014'
bq_dataset = 'stockbit_test1'
gs_bucket = 'stockbit_test'
bucket_name = 'stockbit_test1'
bucket_location = 'ASIA-SOUTHEAST2'
bucket_folder = 'movies'
dataset_folder = f'{airflow_home}/dataset/movies/movies/*.json'
dataset_path = f"{airflow_home}/dataset/movies/movies"
os.environ["KAGGLE_USERNAME"] = Variable.get('kaggle_username')
os.environ["KAGGLE_KEY"] = Variable.get('kaggle_api_key')


# Initiate DAG
dag = DAG(
    'create_movie_dwh',
    default_args=default_args,
    schedule_interval='@once',
    template_searchpath=f'{airflow_home}/dags/sql'
)

# Start the pipeline
pipeline_start = DummyOperator(
    task_id='pipeline_start',
    dag=dag
)

# Download data from Kaggle dataset
## https://www.kaggle.com/edgartanaka1/tmdb-movies-and-series

## Create kaggle creds for user airflow
# create_kaggle_creds = BashOperator(
#     task_id="create_kaggle_creds",
#     dag=dag,
#     bash_command=f"cd {airflow_home} && mkdir {airflow_home}/.kaggle && cat {airflow_home}/creds/kaggle.json > {airflow_home}/.kaggle/kaggle.json && chmod 600 {airflow_home}/.kaggle/kaggle.json"
# )

download_dataset = PythonOperator(
    task_id='download_dataset',
    dag=dag,
    python_callable=kaggle_download_dataset,
    op_kwargs={
        'dataset_name': 'edgartanaka1/tmdb-movies-and-series',
        'dl_path': f'{airflow_home}/dataset',
    }
)

## Delete unused dataset
delete_non_dataset = BashOperator(
    task_id="delete_non_dataset",
    dag=dag,
    bash_command=f"cd {airflow_home}/dataset && rm -rf series"
)

## Delete dataset zip file
delete_zip_dataset = BashOperator(
    task_id='delete_zip_dataset',
    dag=dag,
    bash_command=f"cd {airflow_home}/dataset && rm -rf tmdb-movies-and-series.zip"
)

# Upload selected dataset to GCS
upload_data_to_gcs = DummyOperator(
    task_id="upload_data_to_gcs",
    dag=dag
)

## Create bucket for dataset in GCS
create_gcs_bucket = PythonOperator(
    task_id='create_gcs_bucket',
    dag=dag,
    python_callable=gcs_create_bucket,
    op_kwargs={
        'bucket_name': bucket_name,
        'bucket_location': bucket_location
    }
)
## Upload dataset to GCS
upload_ds_to_gcs = PythonOperator(
    task_id='upload_ds_to_gcs',
    dag=dag,
    python_callable=gcs_upload_dataset_to_bucket,
    op_kwargs={
        'bucket_name': bucket_name,
        'bucket_folder': bucket_folder,
        'dataset_folder': dataset_folder,
        'dataset_path': dataset_path,
        'n_files': 10000
    }
)

## Count uploaded file in GCS
check_uploaded_file_in_gcs = PythonOperator(
    task_id='check_uploaded_file_in_gcs',
    dag=dag,
    python_callable=gcs_check_files,
    op_kwargs={
        'bucket_name': bucket_name,
        'bucket_folder': bucket_folder,
        'dataset_folder': dataset_folder
    }
)


# Ingest dataset from GCS to BigQuery
upload_gcs_to_bq = DummyOperator(
    task_id="upload_gcs_to_bq",
    dag=dag
)

## Create Dataset in BQ
create_dataset = PythonOperator(
    task_id='create_dataset',
    dag=dag,
    python_callable=bq_create_dataset,
    op_kwargs={
        'project_id': project_id,
        'bq_dataset': bq_dataset,
        'bucket_location': bucket_location
    }
)

## Load dataset from GCS to BQ as raw_movie table
load_from_gcs_to_bq = PythonOperator(
    task_id='load_from_gcs_to_bq',
    dag=dag,
    python_callable=bq_load_from_gcs,
    op_kwargs={
        'project_id': project_id,
        'bq_dataset': bq_dataset,
        'table_name': 'raw_movies',
        'bq_schema': f'{airflow_home}/dataset/movies_schema.json',
        'bucket_name': bucket_name,
        'bucket_folder': bucket_folder
    }
)

## Check if raw_table is exists and containts dataset from GCS
check_raw_movies = BigQueryCheckOperator(
    task_id='check_raw_movies',
    dag=dag,
    use_legacy_sql=False,
    location='asia-southeast2',
    sql=f'SELECT count(*) = count(distinct id) from {project_id}.{bq_dataset}.raw_movies',
    bigquery_conn_id='bigquery_default'
)


# Create tables and transform
create_table_start = DummyOperator(
    task_id='create_table_start',
    dag=dag
)

create_movies_media = BigQueryOperator(
    task_id='create_movies_media',
    dag=dag,
    use_legacy_sql = False,
    location='asia-southeast2',
    sql=f'{airflow_home}/dags/sql/create_movies_media.sql',
    params = {
        'bq_dataset': bq_dataset
    },
    bigquery_conn_id='bigquery_default'
)

check_movies_media = BigQueryCheckOperator(
    task_id='check_movies_media',
    dag=dag,
    use_legacy_sql=False,
    location='asia-southeast2',
    sql=f'SELECT count(*) = count(distinct movie_id) FROM {project_id}.{bq_dataset}.movies_media',
    bigquery_conn_id='bigquery_default'
)

create_movies_collection_lists = BigQueryOperator(
    task_id='create_movies_collection_lists',
    dag=dag,
    use_legacy_sql = False,
    location='asia-southeast2',
    sql=f'{airflow_home}/dags/sql/create_movies_collection_lists.sql',
    params = {
        'bq_dataset': bq_dataset
    },
    bigquery_conn_id='bigquery_default'
)

check_movies_collection_lists = BigQueryCheckOperator(
    task_id='check_movies_collection_lists',
    dag=dag,
    use_legacy_sql=False,
    location='asia-southeast2',
    sql=f'SELECT count(*) = count(distinct collection_id) FROM {project_id}.{bq_dataset}.movies_collection_lists',
    bigquery_conn_id='bigquery_default'
)

create_movies_genres = BigQueryOperator(
    task_id='create_movies_genres',
    dag=dag,
    use_legacy_sql = False,
    location='asia-southeast2',
    sql=f'{airflow_home}/dags/sql/create_movies_genres.sql',
    params = {
        'bq_dataset': bq_dataset
    },
    bigquery_conn_id='bigquery_default'
)

check_movies_genres = BigQueryCheckOperator(
    task_id='check_movies_genres',
    dag=dag,
    use_legacy_sql=False,
    location='asia-southeast2',
    sql=f'SELECT count(*) = count(distinct id) FROM {project_id}.{bq_dataset}.movies_genres',
    bigquery_conn_id='bigquery_default'
)

create_movies_fav_by_genre = BigQueryOperator(
    task_id='create_movies_fav_by_genre',
    dag=dag,
    use_legacy_sql = False,
    location='asia-southeast2',
    sql=f'{airflow_home}/dags/sql/create_movies_most_fav_by_genre.sql',
    params = {
        'bq_dataset': bq_dataset
    },
    bigquery_conn_id='bigquery_default'
)

check_movies_fav_by_genre = BigQueryCheckOperator(
    task_id='check_movies_fav_by_genres',
    dag=dag,
    use_legacy_sql=False,
    location='asia-southeast2',
    sql=f'SELECT count(*) FROM {project_id}.{bq_dataset}.movies_most_fav_by_genre',
    bigquery_conn_id='bigquery_default'
)

create_movies_fav_per_year = BigQueryOperator(
    task_id='create_movies_fav_per_year',
    dag=dag,
    use_legacy_sql=False,
    location='asia-southeast2',
    sql=f'{airflow_home}/dags/sql/create_movies_most_fav_per_year.sql',
    params = {
        'bq_dataset': bq_dataset
    },
    bigquery_conn_id='bigquery_default'
)

check_movies_fav_per_year = BigQueryCheckOperator(
    task_id='check_movies_fav_per_year',
    dag=dag,
    use_legacy_sql=False,
    location='asia-southeast2',
    sql=f'SELECT count(*) FROM {project_id}.{bq_dataset}.movies_most_fav_per_year',
    bigquery_conn_id='bigquery_default'
)

create_popular_movies_by_genre = BigQueryOperator(
    task_id='create_popular_movies_by_genre',
    dag=dag,
    use_legacy_sql=False,
    location='asia-southeast2',
    sql=f'{airflow_home}/dags/sql/create_movies_popular_movie_by_genre.sql',
    params = {
        'bq_dataset': bq_dataset
    },
    bigquery_conn_id='bigquery_default'
)

check_popular_movies_by_genre = BigQueryCheckOperator(
    task_id='check_popular_movies_by_genre',
    dag=dag,
    use_legacy_sql=False,
    location='asia-southeast2',
    sql=f'SELECT count(*) FROM {project_id}.{bq_dataset}.movies_popular_movie_by_genre',
    bigquery_conn_id='bigquery_default'
)

create_popular_movies_per_year = BigQueryOperator(
    task_id='create_popular_movies_per_year',
    dag=dag,
    use_legacy_sql=False,
    location='asia-southeast2',
    sql=f'{airflow_home}/dags/sql/create_movies_popular_released_per_year.sql',
    params = {
        'bq_dataset': bq_dataset
    },
    bigquery_conn_id='bigquery_default'
)

check_popular_movies_per_year = BigQueryOperator(
    task_id='check_popular_movies_per_year',
    dag=dag,
    use_legacy_sql=False,
    location='asia-southeast2',
    sql=f'SELECT count(*) FROM {project_id}.{bq_dataset}.movies_popular_released_per_year',
    bigquery_conn_id='bigquery_default'
)

create_movies_production_countries = BigQueryOperator(
    task_id='create_movies_production_countries',
    dag=dag,
    use_legacy_sql=False,
    location='asia-southeast2',
    sql=f'{airflow_home}/dags/sql/create_movies_production_countries.sql',
    params = {
        'bq_dataset': bq_dataset
    },
    bigquery_conn_id='bigquery_default'
)

check_movies_production_countries = BigQueryOperator(
    task_id='check_movies_production_countries',
    dag=dag,
    use_legacy_sql=False,
    location='asia-southeast2',
    sql=f'SELECT count(*) FROM {project_id}.{bq_dataset}.movies_production_countries',
    bigquery_conn_id='bigquery_default'
)

create_movies_spoken_languages = BigQueryOperator(
    task_id='create_movies_spoken_languages',
    dag=dag,
    use_legacy_sql=False,
    location='asia-southeast2',
    sql=f'{airflow_home}/dags/sql/create_movies_spoken_languages.sql',
    params = {
        'bq_dataset': bq_dataset
    },
    bigquery_conn_id='bigquery_default'
)

check_movies_spoken_languages = BigQueryOperator(
    task_id='check_movies_spoken_languages',
    dag=dag,
    use_legacy_sql=False,
    location='asia-southeast2',
    sql=f'SELECT count(*) FROM {project_id}.{bq_dataset}.movies_spoken_language',
    bigquery_conn_id='bigquery_default'
)

create_production_companies_portofolio = BigQueryOperator(
    task_id='create_production_companies_portofolio',
    dag=dag,
    use_legacy_sql=False,
    location='asia-southeast2',
    sql=f'{airflow_home}/dags/sql/create_production_companies_portofolio.sql',
    params = {
        'bq_dataset': bq_dataset
    },
    bigquery_conn_id='bigquery_default'
)

check_production_companies_portofolio = BigQueryOperator(
    task_id='check_production_companies_portofolio',
    dag=dag,
    use_legacy_sql=False,
    location='asia-southeast2',
    sql=f'SELECT count(*) FROM {project_id}.{bq_dataset}.production_companies_portofolio',
    bigquery_conn_id='bigquery_default'
)

# End of pipeline
create_table_end = DummyOperator(
    task_id='create_table_end',
    dag=dag
)


delete_local_dataset = BashOperator(
    task_id='delete_local_dataset',
    dag=dag,
    bash_command=f'rm -rf {airflow_home}/dataset/movies'
)

pipeline_end = DummyOperator(
    task_id='pipeline_end',
    dag=dag
)

# Task Depedencies
pipeline_start >>  download_dataset >> delete_non_dataset >> delete_zip_dataset >> upload_data_to_gcs

upload_data_to_gcs >> create_gcs_bucket >> upload_ds_to_gcs >> check_uploaded_file_in_gcs >> upload_gcs_to_bq

upload_gcs_to_bq >> create_dataset >> load_from_gcs_to_bq >> check_raw_movies >> create_table_start

create_table_start >> [create_movies_genres, create_movies_media, create_movies_collection_lists, create_movies_fav_by_genre, create_movies_fav_per_year, create_popular_movies_by_genre, create_popular_movies_per_year, create_movies_production_countries, create_movies_spoken_languages, create_production_companies_portofolio] 

create_movies_media >> check_movies_media
create_movies_collection_lists >> check_movies_collection_lists
create_movies_genres >> check_movies_genres
create_movies_fav_by_genre >> check_movies_fav_by_genre
create_movies_fav_per_year >> check_movies_fav_per_year
create_popular_movies_by_genre >> check_popular_movies_by_genre
create_popular_movies_per_year >> check_popular_movies_per_year
create_movies_production_countries >> check_movies_production_countries
create_movies_spoken_languages >>check_movies_spoken_languages
create_production_companies_portofolio >> check_production_companies_portofolio

[check_movies_media, check_movies_collection_lists, check_movies_genres, check_movies_fav_by_genre, check_movies_fav_per_year, check_popular_movies_by_genre, check_popular_movies_per_year, check_movies_production_countries, check_movies_spoken_languages, check_production_companies_portofolio] >> create_table_end

create_table_end >> delete_local_dataset >> pipeline_end
