from dataclasses import dataclass
import pathlib
from typing import Type, TypeVar

import dacite
import yaml

from energy_market_etl.loaders.google_storage.config import GoogleCloudStorageConfig


@dataclass
class Config:
    google_storage: GoogleCloudStorageConfig


T = TypeVar("T")


def load_config(config_path: pathlib.Path, config_class: Type[T] = Config) -> T:
    with config_path.open() as f:
        raw_config = yaml.load(f, Loader=yaml.SafeLoader)
    if raw_config is not None:
        return dacite.from_dict(
            data_class=config_class,
            data=raw_config,
        )
