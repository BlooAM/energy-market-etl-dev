from pydantic import BaseModel


class GoogleCloudStorageConfig(BaseModel):
    key_path: str
    bucket_name: str
