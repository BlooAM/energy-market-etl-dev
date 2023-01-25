from typing import Iterable

import pandas as pd

from energy_market_etl.transformers.transformer import Transformer


class SortTransformer(Transformer):
    def __init__(self, sort_by_columns: Iterable[str]) -> None:
        self.sort_by_columns = sort_by_columns

    def transform(self, data_snapshots: pd.DataFrame) -> pd.DataFrame:
        if set(self.sort_by_columns) <= set(data_snapshots.columns):
            data_snapshots_sorted = data_snapshots.sort_values(by=self.sort_by_columns)
            return data_snapshots_sorted
        else:
            raise AttributeError(f'Sort order columns: {self.sort_by_columns} do not match with transformer '
                                 f'data columns')
