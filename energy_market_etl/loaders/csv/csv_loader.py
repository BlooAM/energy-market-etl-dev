import logging
from typing import Optional

import pandas as pd

from energy_market_etl.loaders.loader import Loader


class CsvLoader(Loader):
    def __init__(self, file_name: str, file_path: Optional[str] = '') -> None:
        self.file_name = f'{file_name}.csv'
        self.file_path = file_path if file_path else None

    def load(self, transformed_data: pd.DataFrame) -> None:
        if not transformed_data.empty:
            transformed_data.to_csv(f'{self.file_path}/{self.file_name}') if self.file_path \
                else transformed_data.to_csv(f'{self.file_name}')
        else:
            logging.warning('No data to load. Omitting load phase')

