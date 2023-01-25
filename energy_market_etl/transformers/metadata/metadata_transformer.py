import pandas as pd

from energy_market_etl.transformers.transformer import Transformer


class MetadataTransformer(Transformer):
    def __init__(self, reset_index: bool = True) -> None:
        self.reset_index = reset_index

    def transform(self, raw_data_snapshot: pd.DataFrame) -> pd.DataFrame:
        data_snapshot = raw_data_snapshot.copy()
        data_snapshot.columns = data_snapshot.columns.str.strip()
        if self.reset_index:
            data_snapshot = data_snapshot.reset_index(drop=True)

        return data_snapshot
