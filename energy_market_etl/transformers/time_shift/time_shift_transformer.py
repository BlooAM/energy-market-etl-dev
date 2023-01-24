import datetime as dt
from typing import Dict

import pandas as pd

from energy_market_etl.transformers.transformer import Transformer
from energy_market_etl.utils.date_utils import get_march_switch, get_october_switch


class TimeShiftTransformer(Transformer):
    def __init__(self) -> None:
        pass

    def transform(self, raw_data_snapshots: Dict[dt.datetime, pd.DataFrame]) -> Dict[dt.datetime, pd.DataFrame]:
        data_snapshots = {}
        for datetime, raw_data_snapshot in raw_data_snapshots.items():
            current_date = datetime.date
            current_year = datetime.year
            if current_date == get_march_switch(year=current_year).date():
                data_snapshots[datetime] = self.__apply_march_shift(raw_data_snapshot)
            elif current_date == get_october_switch(year=current_year).date():
                data_snapshots[datetime] = self.__apply_october_shift(raw_data_snapshot)

        return data_snapshots

    def __apply_march_shift(self, data_snapshot: pd.DataFrame):
        transformed_data_snapshot = data_snapshot.copy()

        return transformed_data_snapshot

    def __apply_october_shift(self, data_snapshot: pd.DataFrame):
        transformed_data_snapshot = data_snapshot.copy()

        return transformed_data_snapshot
