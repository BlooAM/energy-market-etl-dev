from typing import Optional

import pandas as pd

from energy_market_etl.loaders.loader import Loader


class CsvLoader(Loader):
    def __init__(self, file_name: str, file_path: Optional[str]) -> None:
        self.file_name = file_name
        self.file_path = file_path if file_path else ''

    def load(self, data: pd.DataFrame) -> None:
        data.to_csv(path=f'{self.file_name}')
