import datetime as dt
import logging
from typing import Callable, Dict
from requests.exceptions import Timeout, RequestException
from urllib.error import HTTPError

import pandas as pd
from retry import retry

from energy_market_etl.extractors.extractor import Extractor
from energy_market_etl.utils.url_utils import UrlProviderFactory


_HTTP_REQUEST_RETRY_DELAY_TIME = 60
_HTTP_REQUEST_RETRY_ATTEMPTS = 5


class PseExtractor(Extractor):
    _PSE_CSV_URL_BASE = 'https://pse.pl/getcsv/-/export/csv'

    def __init__(self, start_date: dt.datetime, end_date: dt.datetime, url_provider_factory: UrlProviderFactory):
        self.start_date = start_date
        self.end_date = end_date
        self.url_provider_factory = url_provider_factory

    def extract(self) -> Dict[dt.datetime, pd.DataFrame]:
        url_provider: Callable = self.__get_url_provider()

        data_snapshots = {}
        for date in pd.date_range(self.start_date, self.end_date):
            logging.debug(f'Extracting PSE data for date: {date.date()}')
            try:
                url = url_provider(date)
                data_snapshots[date] = self.__get_data_snapshot(url)
            except RequestException as e:
                logging.warning(f'{e}. Omitting extraction for date: {date.date()}')
            except Exception as e:
                logging.warning(f'{e}. Omitting extraction for date: {date.date()}')

        return data_snapshots

    @retry(
        exceptions=(HTTPError, Timeout),
        delay=_HTTP_REQUEST_RETRY_DELAY_TIME,
        tries=_HTTP_REQUEST_RETRY_ATTEMPTS
    )
    def __get_data_snapshot(self, url: str) -> pd.DataFrame:
        data_snapshot: pd.DataFrame = pd.read_csv(
            filepath_or_buffer=url,
            sep=';',
            encoding='cp1250',
            decimal=',',
        )
        return data_snapshot

    def __get_url_provider(self) -> Callable:
        url_provider = self.url_provider_factory.get_url_provider(
            url_base=PseExtractor._PSE_CSV_URL_BASE,
        )
        return url_provider
