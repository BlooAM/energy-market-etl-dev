import logging

from energy_market_etl.loaders.loader import Loader
from energy_market_etl.loaders.google_storage.config import GoogleCloudStorageConfig
from energy_market_etl.loaders.google_storage.client.google_cloud_storage_client import create_gcs_client


class GoogleCloudStorageLoader(Loader):
    def __init__(self, config: GoogleCloudStorageConfig) -> None:
        client = create_gcs_client(key_path=config.key_path)
        self._bucket = client.bucket(config.bucket_name)

    def load(self, transformed_data: pd.DataFrame) -> None:
        if transformed_data.empty:
            logging.warning('No data to load. Omitting load phase')
        else:
            blob = self._bucket.blob('test.csv')
            blob.upload_from_string(transformed_data.to_csv(), 'text/csv')
