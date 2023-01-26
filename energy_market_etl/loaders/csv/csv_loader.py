import logging
import os

import pandas as pd

from energy_market_etl.loaders.loader import Loader
from energy_market_etl.utils.dynamic_etl_loader import PROJ_DIR


class CsvLoader(Loader):
    def __init__(self, file_name: str, file_path: str = '') -> None:
        self.file_name = f'{file_name}.csv'
        self.file_path = file_path

    def load(self, transformed_data: pd.DataFrame) -> None:
        if transformed_data.empty:
            logging.warning('No data to load. Omitting load phase')
        else:
            if self.file_path:
                transformed_data.to_csv(os.path.join(f'{self.file_path}', f'{self.file_name}'))
            else:
                transformed_data.to_csv(os.path.join(f'{PROJ_DIR}', 'reports', f'{self.file_name}'))
