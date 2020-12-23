import os
from kaggle.api.kaggle_api_extended import KaggleApi

airflow_home = "/usr/local/airflow"
file_name = 'tmdb-movies-and-series.zip'

def kaggle_auth():
    api = KaggleApi()
    api.authenticate()
    return api


def kaggle_download_dataset(dataset_name, dl_path, file_name):
    print(f"Downloading {dataset_name} from Kaggle")
    kg_api = kaggle_auth()
    kg_api.authenticate()
    try:
        kg_api.dataset_download_files(dataset=dataset_name, file_name=file_name, unzip=True)
        print(f"Download dataset {dataset_name}: Done")
    except Exception as e:
        print(f"error: {e}")
