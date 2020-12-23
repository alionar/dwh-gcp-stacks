from google.cloud import storage
import glob
import random
import os

# VARIABLE TEST
bucket_name = 'stockbit_test'
creds_file = './creds/gcs_client_secret.json'
bucket_location = "ASIA-SOUTHEAST2"
dataset_folder = './dataset/movies/movies/*.json'
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
        print(f"error: {e}")


def gcs_upload_dataset_to_bucket(bucket_name=bucket_name, bucket_folder=bucket_folder, dataset_folder=dataset_folder, n_files=2000):
    print(f'Select json files to upload as dataset: {n_files} files')
    json_files = random.sample(glob.glob(dataset_folder), n_files)

    print(f'Delete another json files: {len(os.listdir(dataset_folder))-n_files} files')
    for filename in os.listdir(dataset_folder):
        if filename not in json_files:
            os.remove(filename)

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
        print(f"error: {e}")


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
        print(f"error: {e}")
