from energy_market_etl.loaders.loader import Loader
from energy_market_etl.loaders.google_storage.config import GoogleCloudStorageConfig
from energy_market_etl.loaders.google_storage.client.google_cloud_storage_client import create_gcs_client


class GoogleCloudStorageLoader(Loader):
    def __init__(self, config: GoogleCloudStorageConfig):
        client = create_gcs_client(key_path=config.key_path)
        self._bucket = client.bucket(config.bucket_name)

    def load(self):
        blob = self._bucket.blob('test.csv')
        blob.upload_from_string(df.to_csv(), 'text/csv')
