from energy_market_etl.loaders.loader import Loader
from energy_market_etl.loaders.google_storage.config import GoogleCloudStorageConfig


class GoogleCloudStorageLoader(Loader):
    def __init__(self, config: GoogleCloudStorageConfig):
        client = self._create_client(config.key_path)
        self._bucket = client.bucket(config.bucket_name)

    def load(self):
        pass
