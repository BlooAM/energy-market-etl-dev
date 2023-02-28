import logging

import pandas as pd

from energy_market_etl.loaders.loader import Loader
from energy_market_etl.loaders.google_storage.config import GoogleCloudStorageConfig
from energy_market_etl.loaders.google_storage.client.google_cloud_storage_client import create_gcs_client


class GoogleCloudStorageLoader(Loader):
    def __init__(self, config: GoogleCloudStorageConfig, file_name: str) -> None:
        self.file_name = f'{file_name}.csv'
        client = create_gcs_client(key_path=config.key_path)
        self._bucket = client.bucket(config.bucket_name)

    def load(self, transformed_data: pd.DataFrame) -> None:
        if transformed_data.empty:
            logging.warning('No data to load. Omitting load phase')
        else:
            blob = self._bucket.blob(f'{self.file_name}')
            if blob.exists():
                logging.warning("File: %s has already existed and will be overridden", self.file_name)
            blob.upload_from_string(transformed_data.to_csv(), 'text/csv')
            if blob.exists():
                logging.info("File: %s saved successfully to bucket: %s", self.file_name, self._bucket)
