import datetime as dt
from typing import List, Dict, Union

import pandas as pd

from energy_market_etl.transformers.transformer import Transformer


class NumericCastTransformer(Transformer):
    def __init__(self, columns_to_cast: Union[str, List[str]] = 'all'):
        self.columns_to_cast = columns_to_cast

    def transform(self, raw_data_snapshots: Dict[dt.datetime, pd.DataFrame]) -> \
            Union[Dict[dt.datetime, pd.DataFrame], pd.DataFrame]:
        data_snapshots = {}
        for datetime, raw_data_snapshots in raw_data_snapshots.items():
            pass

        return data_snapshots