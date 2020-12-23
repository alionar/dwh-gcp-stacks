import os
from kaggle.api_api_extended import KaggleApi

airflow_home = "/usr/local/airflow"
file_name = 'tmdb-movies-and-series.zip'

def kaggle_auth(username, api_key):
    # os.environ["KAGGLE_USERNAME"] = username
    # os.environ["KAGGLE_KEY"] = api_key

    api = KaggleApi()
    api.authenticate()
    return api


def kaggle_download_dataset(dataset_name, dl_path, file_name, username, api_key):
    print(f"Downloading {dataset_name} from Kaggle")
    api = kaggle_auth(username, api_key)
    try:
        api.dataset_download_files(dataset=dataset_name, file_name=file_name, unzip=True)
        print(f"Download dataset {dataset_name}: Done")
    except Exception as e:
        print(f"error: {e}")
