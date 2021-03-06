from google.cloud import storage
import glob
import random
import os
import multiprocessing
from joblib import Parallel, delayed

# VARIABLE TEST
airflow_home = '/usr/local/airflow'
bucket_name = 'stockbit_test'
creds_file = f'{airflow_home}/creds/gcs_client_secret.json'
bucket_location = "ASIA-SOUTHEAST2"
dataset_folder = f'{airflow_home}/dataset/movies/movies/*.json'
dataset_path = f"{airflow_home}/dataset/movies/movies"
bucket_folder = 'movies'


def gcs_client(creds_file=creds_file):
    if os.path.exists(creds_file):
        try:
            client = storage.Client.from_service_account_json(creds_file)
            return client
        except Exception as e:
            print(f"error: {e}")
    else:
        raise Exception(f"Cannot locate {creds_file}")


def gcs_create_bucket(bucket_name=bucket_name, bucket_location=bucket_location):
    print(f'Creating new bucket: {bucket_name}')
    try:
        client = gcs_client(creds_file=creds_file)

        bucket = client.bucket(bucket_name)
        new_bucket = client.create_bucket(bucket, location=bucket_location)

        print(f"Successfully created new bucket {new_bucket.name} in {new_bucket.location}")
    except Exception as e:
        raise Exception(f"error: {e}")


def delete_files(filepath):
    if os.path.exists(filepath):
        os.remove(filepath)
    else:
        pass


def gcs_upload_dataset_to_bucket(bucket_name=bucket_name, bucket_folder=bucket_folder, dataset_path=dataset_path, dataset_folder=dataset_folder, n_files=2000):
    print(f'Select json files to upload as dataset: {n_files} files')
    files = glob.glob(dataset_folder)
    json_files = random.sample(files, n_files)

    if len(json_files) > 0:
        print(f'Delete another json files: {len(files)-len(json_files)} files')
        exclude_dataset = set(files).difference(json_files)
        num_cores = multiprocessing.cpu_count()
        results = Parallel(n_jobs=num_cores)(delayed(delete_files)(filepath) for filepath in exclude_dataset)

        print(f"There're {len(json_files)} files ready to upload to GCS")
        print(f'Get bucket: {bucket_name}')
        try:
            client = gcs_client(creds_file=creds_file)
            bucket = client.get_bucket(bucket_name)

            print(f'Uploading json files to "{bucket_name}" bucket')
            for js in json_files:
                blob = bucket.blob(f"{bucket_folder}/{os.path.basename(js)}")
                blob.upload_from_filename(js)
            print("Upload: Done")
        except Exception as e:
            raise Exception(f"error: {e}")
    else:
        raise Exception('No dataset to upload')


def gcs_check_files(dataset_folder, bucket_name, bucket_folder):
    try:
        client = gcs_client(creds_file=creds_file)
        bucket = client.get_bucket(bucket_name)

        bucket_files = bucket.list_blobs(prefix=bucket_folder)
        count_bucket_files = len(list(bucket_files))
        count_ds_files = len(glob.glob(dataset_folder))

        if count_bucket_files == count_ds_files:
            print(f'Checking upload result: verified ({count_bucket_files} dataset)')
        else:
            raise Exception(f"Checking upload result: failed\nReason: Different files count (in bucket: {count_bucket_files}, in dataset: {count_ds_files})")
    except Exception as e:
        raise Exception(f"error: {e}")
