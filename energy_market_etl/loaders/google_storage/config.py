from dataclasses import dataclass


@dataclass
class GoogleCloudStorageConfig:
    key_path: str
    bucket_name: str
