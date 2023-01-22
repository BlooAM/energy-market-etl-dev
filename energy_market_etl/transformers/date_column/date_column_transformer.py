import datetime as dt
from typing import Dict

import pandas as pd

from energy_market_etl.transformers.transformer import Transformer


class DateColumnTransformer(Transformer):
    def __init__(self, date_column_name: str = 'Data'):
        self.date_column_name = date_column_name

    def transform(self, raw_data_snapshots: Dict[dt.datetime, pd.DataFrame]) -> Dict[dt.datetime, pd.DataFrame]:
        data_snapshots = {}
        for datetime, raw_data_snapshot in raw_data_snapshots.items():
            date = datetime.date()
            data_snapshots[datetime] = raw_data_snapshot.copy()
            data_snapshots[datetime].insert(
                loc=0,
                column=self.date_column_name,
                value=str(date),
                allow_duplicates=True,
            )

        return data_snapshots
