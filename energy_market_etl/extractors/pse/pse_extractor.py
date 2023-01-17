import datetime as dt

import pandas as pd

from energy_market_etl.extractors.extractor import Extractor
from energy_market_etl.extractors.pse.utils import _PSE_DATA_TYPE_URL_MAPPER


class PseExtractor(Extractor):
    def __int__(self, start_date: dt.datetime, end_date: dt.datetime, data_type: str):
        self.start_date = start_date
        self.end_date = end_date
        self.__url_getter = _PSE_DATA_TYPE_URL_MAPPER.get(data_type)
        if self.__url_getter is None:
            raise NotImplementedError(f'data type: {data_type} not implemented') #TODO: replace with custom Exception

    def extract(self) -> pd.DataFrame:
        data_snapshots = []
        for date in pd.date_range(self.start_date, self.end_date):
            data_snapshot = self.__get_data_snapshot(date) #TODO: handle possible exceptions here
            data_snapshots.append(data_snapshot)

        return pd.concat(data_snapshots)

    def __get_data_snapshot(self, date: dt.datetime) -> pd.DataFrame:
        url = self.__url_getter(date)
        df = pd.read_csv(
            filepath_or_buffer=url,
            sep=';',
            encoding='cp1250',
        )
        return df
