import os

from google.cloud import storage


class GoogleStorageKeyNotFound(Exception):
    def __init__(self, path: str):
        self.message = f'Google storage key has not been found for path: {path}.' \
                       f' Please provide the correct path in the configuration file.'
        super().__init__(self.message)


def create_gcs_client(key_path: str) -> storage.Client:
    if key_path is None:
        return storage.Client()
    else:
        if not os.path.exists(key_path):
            raise GoogleStorageKeyNotFound(path=key_path)
        return storage.Client.from_service_account_json(key_path)
