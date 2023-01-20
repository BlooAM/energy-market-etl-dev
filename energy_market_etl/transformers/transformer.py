from abc import ABC, abstractmethod
import datetime as dt
from typing import Dict, Union

import pandas as pd


class Transformer(ABC):
    @abstractmethod
    def transform(self, raw_data_snapshots: Dict[dt.datetime, pd.DataFrame]) -> \
            Union[Dict[dt.datetime, pd.DataFrame], pd.DataFrame]:
        raise NotImplementedError('Transformer has to implement `transform` method')
