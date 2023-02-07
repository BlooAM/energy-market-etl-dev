from abc import ABC, abstractmethod
import datetime as dt
from typing import Dict

import pandas as pd


class Etl(ABC):
    def __init__(
            self,
            start_date: dt.datetime,
            end_date: dt.datetime,
            report_type: str,
    ) -> None:
        self.start_date = start_date
        self.end_date = end_date
        if self.start_date == self.end_date:
            self.report_name = f'{report_type}_{start_date.date()}'
        else:
            self.report_name = f'{report_type}_{start_date.date()}_{end_date.date()}'
        self.__extracted_data: Dict[dt.datetime, pd.DataFrame] = {}
        self.__transformed_data: pd.DataFrame = pd.DataFrame()

    @abstractmethod
    def extract(self) -> None:
        raise NotImplementedError('`extract` method not implemented in ETL object')

    @abstractmethod
    def transform(self) -> None:
        raise NotImplementedError('`transform` method not implemented in ETL object')

    @abstractmethod
    def load(self) -> None:
        raise NotImplementedError('`load` method not implemented in ETL object')