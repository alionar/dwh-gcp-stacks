import os
from kaggle.api.kaggle_api_extended import KaggleApi

airflow_home = "/usr/local/airflow"
file_name = 'tmdb-movies-and-series.zip'

def kaggle_auth():
    api = KaggleApi()
    api.authenticate()
    return api


def kaggle_download_dataset(dataset_name, dl_path):
    print(f"Downloading {dataset_name} from Kaggle")
    kg_api = kaggle_auth()
    kg_api.authenticate()
    try:
        kg_api.dataset_download_files(dataset_name, path=dl_path, unzip=True, quiet=False)
        print(f"Download dataset {dataset_name}: Done")
    except Exception as e:
        raise Exception(f"error: {e}")
