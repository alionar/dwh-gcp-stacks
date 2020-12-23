from google.cloud import bigquery
from google.cloud import storage
from google.oauth2 import service_account
from helper.gcs import gcs_client
import os

# VARIABLE TEST
airflow_home = '/usr/local/airflow'
bq_creds_file = f'{airflow_home}/creds/gcs_bq_client_secret.json'
bucket_name = 'stockbit_test'
bucket_folder = 'movies'
bucket_location = "ASIA-SOUTHEAST2"
project_id = 'certain-region-299014'
bq_dataset = 'stockbit_test'
bq_schema = f'{airflow_home}/dataset/movies_schema.json'
table_name = 'raw_movies'

def bq_client(creds_file=bq_creds_file):
    if os.path.exists(creds_file):
        try:
            credentials = service_account.Credentials.from_service_account_file(
                bq_creds_file, 
                scopes=["https://www.googleapis.com/auth/cloud-platform"],
            )
            client = bigquery.Client(credentials=credentials, project=credentials.project_id,)
            return client
        except Exception as e:
            print(f"error: {e}")
    else:
        raise Exception(f"Cannot locate {creds_file}")


def bq_create_dataset(project_id, bq_dataset, bucket_location):
    client = bq_client(bq_creds_file)
    try:
        print(f"Creating dataset: {project_id}.{bq_dataset}")
        dataset_id = f"{client.project}.{bq_dataset}"
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = bucket_location
        dataset = client.create_dataset(dataset, timeout=30)
        print(f"Created dataset {client.project}.{dataset.dataset_id}")

    except Exception as e:
        raise Exception(f"error: {e}")


def bq_load_from_gcs(project_id, bq_dataset, table_name, bq_schema, bucket_name, bucket_folder):
    client = bq_client(creds_file=bq_creds_file)
    try:
        # Load to BigQuery
        print(f"Loading data from GCS to BQ: {project_id}.{bq_dataset}")
        table_id = f"{project_id}.{bq_dataset}.{table_name}"

        job_config = bigquery.LoadJobConfig(
            schema=client.schema_from_json(file_or_path=bq_schema),
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        )

        bucket_client = gcs_client(creds_file='./creds/gcs_client_secret.json')
        bucket = bucket_client.get_bucket(bucket_name)
        
        for blob in bucket.list_blobs(prefix=bucket_folder):
            uri = f"gs://{bucket_name}/{blob.name}"

            load_job = client.load_table_from_uri(
                uri,
                table_id,
                location = bucket_location,
                job_config=job_config
            )
            load_job.result()
        destination_table = client.get_table(table_id)
        print(f"Table {table_name} loaded: {destination_table.num_rows}")
    except Exception as e:
        raise Exception(f"error: {e}")

