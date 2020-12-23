from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from scripts.gcs import *
from scripts.bq import *
import os

default_args = {
    'owner': 'Aulia Lionar',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

project_id = 'certain-region-299014'
dataset = 'stockbit_test'
gs_bucket = 'stockbit_test'
kaggle_username = Variable.get('kaggle_username')
kaggle_api_key = Variable.get('kaggle_api_key')

dag = DAG(
    'create_movie_dwh',
    default_args=default_args,
    schedule_interval='@once'
)

# Start the pipeline
pipeline_start = DummyOperator(
    task_id='pipeline_start',
    dag=dag
)

# Download data from Kaggle dataset
## https://www.kaggle.com/edgartanaka1/tmdb-movies-and-series
download_data = BashOperator(
    task_id="download_dataset",
    dag=dag,
    bash_command=f"export KAGGLE_USERNAME={kaggle_username} && export KAGGLE_KEY={kaggle_api_key} && cd ./dataset && kaggle datasets download -d edgartanaka1/tmdb-movies-and-series"
)

## Unzip dataset file
unzip_dataset = BashOperator(
    task_id="unzip_dataset",
    dag=dag,
    bash_command="cd ./dataset && unzip ./tmdb-movies-and-series.zip 'movies/movies*'"
)

## Delete dataset zip file
delete_zip_dataset = BashOperator(
    task_id='delete_zip_dataset',
    dag=dag,
    bash_command="cd ./dataset && rm -rf tmdb-movies-and-series.zip"
)

# Upload selected dataset to GCS
upload_data_to_gcs = DummyOperator(
    task_id="upload_data_to_gcs",
    dag=dag
)

bucket_name = 'stockbit_test'
bucket_location = 'ASIA-SOUTHEAST2'
bucket_folder = 'movies'
dataset_folder = './dataset/movies/movies/*.json'


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

create_dataset = PythonOperator(
    task_id='create_dataset',
    dag=dag,
    python_callable=bq_create_dataset,
    op_kwargs={
        'project_id': project_id,
        'bq_dataset': dataset,
        'bucket_location': bucket_location
    }
)

load_from_gcs_to_bq = PythonOperator(
    task_id='load_from_gcs_to_bq',
    dag=dag,
    python_callable=bq_load_from_gcs,
    op_kwargs={
        'project_id': project_id,
        'bq_dataset': dataset,
        'table_name': 'raw_movies',
        'bq_schema': './dataset/movies_schema.json',
        'bucket_name': bucket_name,
        'bucket_folder': bucket_folder
    }
)

check_raw_movies = BigQueryCheckOperator(
    task_id='check_raw_movies',
    dag=dag,
    use_legacy_sql=False,
    sql=f'SELECT count(*) = count(distinct id) from {project_id}.{dataset}.raw_movies',
    bigquery_conn_id='bigquery_default'
)


# Create tables and transform
create_table = DummyOperator(
    task_id='create_table',
    dag=dag
)

create_movies_media = BigQueryOperator(
    task_id='create_movies_media',
    dag=dag,
    use_legacy_sql = False,
    location='asia-southeast2',
    sql='./sql/create_movies_media.sql',
    bigquery_conn_id='bigquery_default'
)

check_movies_media = BigQueryCheckOperator(
    task_id='check_movies_media',
    dag=dag,
    use_legacy_sql=False,
    sql=f'SELECT count(*) = count(distinct movie_id) FROM {project_id}.{dataset}.movies_media',
    bigquery_conn_id='bigquery_default'
)

create_movies_collection_lists = BigQueryOperator(
    task_id='create_movies_collection_lists',
    dag=dag,
    use_legacy_sql = False,
    location='asia-southeast2',
    sql='./sql/create_movies_collection_lists.sql',
    bigquery_conn_id='bigquery_default'
)

check_movies_collection_lists = BigQueryCheckOperator(
    task_id='check_movies_collection_lists',
    dag=dag,
    use_legacy_sql=False,
    sql=f'SELECT count(*) = count(distinct collection_id) FROM {project_id}.{dataset}.movies_collection_lists',
    bigquery_conn_id='bigquery_default'
)

create_movies_genres = BigQueryOperator(
    task_id='create_movies_genres',
    dag=dag,
    use_legacy_sql = False,
    location='asia-southeast2',
    sql='./sql/create_movies_genres.sql',
    bigquery_conn_id='bigquery_default'
)

check_movies_genres = BigQueryCheckOperator(
    task_id='check_movies_genres',
    dag=dag,
    use_legacy_sql=False,
    sql=f'SELECT count(*) = count(distinct id) FROM {project_id}.{dataset}.movies_genres',
    bigquery_conn_id='bigquery_default'
)

create_movies_fav_by_genre = BigQueryOperator(
    task_id='create_movies_fav_by_genre',
    dag=dag,
    use_legacy_sql = False,
    location='asia-southeast2',
    sql='./sql/create_movies_most_fav_by_genre.sql',
    bigquery_conn_id='bigquery_default'
)

check_movies_fav_by_genre = BigQueryCheckOperator(
    task_id='check_movies_fav_by_genres',
    dag=dag,
    use_legacy_sql=False,
    sql=f'SELECT count(*) FROM {project_id}.{dataset}.movies_most_fav_by_genre',
    bigquery_conn_id='bigquery_default'
)

create_movies_fav_per_year = BigQueryOperator(
    task_id='create_movies_fav_per_year',
    dag=dag,
    use_legacy_sql=False,
    location='asia-southeast2',
    sql='./sql/create_movies_most_fav_per_year.sql',
    bigquery_conn_id='bigquery_default'
)

check_movies_fav_per_year = BigQueryCheckOperator(
    task_id='check_movies_fav_per_year',
    dag=dag,
    use_legacy_sql=False,
    sql=f'SELECT count(*) FROM {project_id}.{dataset}.movies_most_fav_per_year',
    bigquery_conn_id='bigquery_default'
)

create_popular_movies_by_genre = BigQueryOperator(
    task_id='create_popular_movies_by_genre',
    dag=dag,
    use_legacy_sql=False,
    location='asia-southeast2',
    sql='./sql/create_movies_popular_movie_by_genre.sql',
    bigquery_conn_id='bigquery_default'
)

check_popular_movies_by_genre = BigQueryCheckOperator(
    task_id='check_popular_movies_by_genre',
    dag=dag,
    use_legacy_sql=False,
    sql=f'SELECT count(*) FROM {project_id}.{dataset}.movies_popular_movie_by_genre',
    bigquery_conn_id='bigquery_default'
)

create_popular_movies_per_year = BigQueryOperator(
    task_id='create_popular_movies_per_year',
    dag=dag,
    use_legacy_sql=False,
    location='asia-southeast2',
    sql='./sql/create_movies_popular_released_per_year.sql',
    bigquery_conn_id='bigquery_default'
)

check_popular_movies_per_year = BigQueryOperator(
    task_id='check_popular_movies_per_year',
    dag=dag,
    use_legacy_sql=False,
    sql=f'SELECT count(*) FROM {project_id}.{dataset}.movies_popular_released_per_year',
    bigquery_conn_id='bigquery_default'
)

create_movies_production_countries = BigQueryOperator(
    task_id='create_movies_production_countries',
    dag=dag,
    use_legacy_sql=False,
    location='asia-southeast2',
    sql='./sql/create_movies_production_countries.sql',
    bigquery_conn_id='bigquery_default'
)

check_movies_production_countries = BigQueryOperator(
    task_id='check_movies_production_countries',
    dag=dag,
    use_legacy_sql=False,
    sql=f'SELECT count(*) FROM {project_id}.{dataset}.movies_production_countries',
    bigquery_conn_id='bigquery_default'
)

create_movies_spoken_languages = BigQueryOperator(
    task_id='create_movies_spoken_languages',
    dag=dag,
    use_legacy_sql=False,
    location='asia-southeast2',
    sql='./sql/create_movies_spoken_languages.sql',
    bigquery_conn_id='bigquery_default'
)

check_movies_spoken_languages = BigQueryOperator(
    task_id='check_movies_spoken_languages',
    dag=dag,
    use_legacy_sql=False,
    sql=f'SELECT count(*) FROM {project_id}.{dataset}.movies_spoken_language',
    bigquery_conn_id='bigquery_default'
)

create_production_companies_portofolio = BigQueryOperator(
    task_id='create_production_companies_portofolio',
    dag=dag,
    use_legacy_sql=False,
    location='asia-southeast2',
    sql='./sql/create_production_companies_portofolio.sql',
    bigquery_conn_id='bigquery_default'
)

check_production_companies_portofolio = BigQueryOperator(
    task_id='check_production_companies_portofolio',
    dag=dag,
    use_legacy_sql=False,
    sql=f'SELECT count(*) FROM {project_id}.{dataset}.production_companies_portofolio',
    bigquery_conn_id='bigquery_default'
)

# End of pipeline
delete_local_dataset = BashOperator(
    task_id='delete_local_dataset',
    dag=dag,
    bash_command = 'rm -rf ./dataset/movies'
)

pipeline_end = DummyOperator(
    task_id='pipeline_end',
    dag=dag
)

# Task Depedencies
pipeline_start >> download_data >> unzip_dataset >> delete_zip_dataset >> upload_data_to_gcs

upload_data_to_gcs >> create_gcs_bucket >> upload_ds_to_gcs >> check_uploaded_file_in_gcs >> upload_gcs_to_bq

upload_gcs_to_bq >> create_dataset >> load_from_gcs_to_bq >> check_raw_movies >> create_table

create_table >> [create_movies_media, create_movies_collection_lists, create_movies_fav_by_genre, create_movies_fav_per_year, create_popular_movies_by_genre, create_popular_movies_per_year, create_movies_production_countries, create_movies_spoken_languages, create_production_companies_portofolio] 

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

[check_movies_media, check_movies_collection_lists, check_movies_genres, check_movies_fav_by_genre, check_movies_fav_per_year, check_popular_movies_by_genre, check_popular_movies_per_year, check_movies_production_countries, check_movies_spoken_languages, check_production_companies_portofolio] >> delete_local_dataset >> pipeline_end
