import datetime as dt
from typing import Dict
from urllib.error import URLError, HTTPError

import pandas as pd
from retry import retry

from energy_market_etl.extractors.extractor import Extractor
from energy_market_etl.extractors.pse.utils import _PSE_DATA_TYPE_URL_MAPPER


_HTTP_REQUEST_RETRY_DELAY_TIME = 60
_HTTP_REQUEST_RETRY_ATTEMPTS = 5


class PseExtractor(Extractor):
    _PSE_CSV_URL_BASE = 'https://pse.pl/getcsv/-/export/csv'

    def __init__(self, start_date: dt.datetime, end_date: dt.datetime, data_type: str):
        self.start_date = start_date
        self.end_date = end_date
        self.__url_getter = _PSE_DATA_TYPE_URL_MAPPER.get(data_type)
        if self.__url_getter is None:
            raise NotImplementedError(f'data type: {data_type} not implemented') #TODO: replace with custom Exception

    def extract(self) -> Dict[dt.datetime, pd.DataFrame]:
        data_snapshots = {}
        for date in pd.date_range(self.start_date, self.end_date):
            try:
                data_snapshots[date] = self.__get_data_snapshot(date)
            except HTTPError as e:
                print(e)  # TODO: raise custom error
                return {}
            except URLError as e:
                print(e)  # TODO: raise custom error
                return {}
        return data_snapshots

    @retry(HTTPError, delay=_HTTP_REQUEST_RETRY_DELAY_TIME, tries=_HTTP_REQUEST_RETRY_ATTEMPTS)
    def __get_data_snapshot(self, date: dt.datetime) -> pd.DataFrame:
        url: str = self.__url_getter(date)
        data_snapshot: pd.DataFrame = pd.read_csv(
            filepath_or_buffer=url,
            sep=';',
            encoding='cp1250',
        )
        return data_snapshot


if __name__ == '__main__':
    _start_date = dt.datetime(2020, 11, 15)
    _end_date = dt.datetime(2020, 11, 17)
    _data_type = 'system_data'
    # _data_type = 'unit_generation_data'

    extractor = PseExtractor(
        start_date=_start_date,
        end_date=_end_date,
        data_type=_data_type,
    )
    data = extractor.extract()
    key_sample = list(data.keys())[0]
    data_sample = data.get(key_sample)
