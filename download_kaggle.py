import os
from kaggle.api.kaggle_api_extended import KaggleApi
from loguru import logger

def download_kaggle_dataset(owner_slug: str, dataset_name: str, download_path: str):
    api = KaggleApi()
    api.authenticate()

    os.makedirs(download_path, exist_ok=True)

    api.dataset_download_files(
        f"{owner_slug}/{dataset_name}",
        path=download_path,
        unzip=True
    )
    logger.info(f"Dataset downloaded to: {download_path}")
