import datetime as dt
import logging
from typing import Dict, Iterable

import pandas as pd

from energy_market_etl.transformers.transformer import Transformer


class VerticalStackTransformer(Transformer):
    def __init__(self, post_stack_sort: bool = False, sort_order_columns: Iterable = []) -> None:
        self.post_stack_sort = post_stack_sort
        self.sort_order_columns = sort_order_columns

    def transform(self, raw_data_snapshots: Dict[dt.datetime, pd.DataFrame]) -> pd.DataFrame:
        snapshots_to_stack = raw_data_snapshots.values()
        if not snapshots_to_stack:
            logging.warning('')
            return pd.DataFrame()
        else:
            data_snapshots = pd.concat(snapshots_to_stack, axis=0)

        if self.post_stack_sort:
            if set(self.sort_order_columns) <= set(data_snapshots.columns):
                data_snapshots = data_snapshots.sort_values(by=self.sort_order_columns)
            else:
                raise AttributeError(f'Sort order columns: {self.sort_order_columns} do not match with transformer '
                                     f'data columns')
        return data_snapshots
