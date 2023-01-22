import datetime as dt
import logging
from typing import Dict, Optional

import pandas as pd

from energy_market_etl.transformers.transformer import Transformer


class StackTransformer(Transformer):
    def __init__(self, stack_dimension: Optional[str] = 'vertical') -> None:
        self.stack_dimension = stack_dimension

    def transform(self, raw_data_snapshots: Dict[dt.datetime, pd.DataFrame]) -> pd.DataFrame:
        snapshots_to_stack = raw_data_snapshots.values()
        if not snapshots_to_stack:
            logging.warning('No data to concatenate. Omitting stacking transformation.')
            return pd.DataFrame()
        else:
            axis = 0 if self.stack_dimension == 'vertical' else 1
            data_snapshots = pd.concat(snapshots_to_stack, axis=axis)
            return data_snapshots


        if self.post_stack_sort:
            if set(self.sort_order_columns) <= set(data_snapshots.columns):
                data_snapshots = data_snapshots.sort_values(by=self.sort_order_columns)
            else:
                raise AttributeError(f'Sort order columns: {self.sort_order_columns} do not match with transformer '
                                     f'data columns')

