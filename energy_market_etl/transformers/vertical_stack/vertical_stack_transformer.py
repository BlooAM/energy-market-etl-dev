import datetime as dt
from typing import Dict, Iterable

import pandas as pd

from energy_market_etl.transformers.transformer import Transformer


class VerticalStackTransformer(Transformer):
    def __init__(self, post_stack_sort: bool = False, sort_order_columns: Iterable = []) -> None:
        self.post_stack_sort = post_stack_sort
        self.sort_order_columns = sort_order_columns

    def transform(self, raw_data_snapshots: Dict[dt.datetime, pd.DataFrame]) -> pd.DataFrame:
        data_snapshots = pd.concat(raw_data_snapshots, axis=0)
        if self.post_stack_sort:
            if set(self.sort_order_columns) <= set(data_snapshots.columns):
                data_snapshots = data_snapshots.sort_values(by=self.sort_order_columns)
            else:
                raise AttributeError(f'Sort order columns: {self.sort_order_columns} do not match with transformer '
                                     f'data columns')
        return data_snapshots
