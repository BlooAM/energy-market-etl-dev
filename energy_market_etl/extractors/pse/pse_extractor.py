import datetime as dt
from typing import Callable, Dict
from urllib.error import URLError, HTTPError

import pandas as pd
from retry import retry

from energy_market_etl.extractors.extractor import Extractor
from energy_market_etl.utils.url_utils import UrlProviderFactory


_HTTP_REQUEST_RETRY_DELAY_TIME = 60
_HTTP_REQUEST_RETRY_ATTEMPTS = 5


class PseExtractor(Extractor):
    _PSE_CSV_URL_BASE = 'https://pse.pl/getcsv/-/export/csv'

    def __init__(self, start_date: dt.datetime, end_date: dt.datetime, data_access_endpoint: str):
        self.start_date = start_date
        self.end_date = end_date
        self.data_access_endpoint = data_access_endpoint
        self.__url_provider_factory = UrlProviderFactory(url_type='endpoint')

    def extract(self) -> Dict[dt.datetime, pd.DataFrame]:
        url_provider: Callable = self.__get_url_provider()
        data_snapshots = {}
        for date in pd.date_range(self.start_date, self.end_date):
            try:
                url = url_provider(date)
                data_snapshots[date] = self.__get_data_snapshot(url)
            except HTTPError as e:
                print(e)  # TODO: raise custom error
                return {}
            except URLError as e:
                print(e)  # TODO: raise custom error
                return {}
        return data_snapshots

    @retry(HTTPError, delay=_HTTP_REQUEST_RETRY_DELAY_TIME, tries=_HTTP_REQUEST_RETRY_ATTEMPTS)
    def __get_data_snapshot(self, url: str) -> pd.DataFrame:
        data_snapshot: pd.DataFrame = pd.read_csv(
            filepath_or_buffer=url,
            sep=';',
            encoding='cp1250',
        )
        return data_snapshot

    def __get_url_provider(self) -> Callable:
        url_provider = self.__url_provider_factory.get_url_provider(
            url_base=PseExtractor._PSE_CSV_URL_BASE,
            endpoint=self.data_access_endpoint,
        )
        return url_provider


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
