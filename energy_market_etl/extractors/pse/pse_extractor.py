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

    def __init__(self, start_date: dt.datetime, end_date: dt.datetime, url_provider_factory: UrlProviderFactory):
        self.start_date = start_date
        self.end_date = end_date
        self.url_provider_factory = url_provider_factory

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
            decimal=',',
        )
        return data_snapshot

    def __get_url_provider(self) -> Callable:
        url_provider = self.url_provider_factory.get_url_provider(
            url_base=PseExtractor._PSE_CSV_URL_BASE,
        )
        return url_provider


if __name__ == '__main__':
    period_start_date = dt.datetime(2022, 3, 26)
    period_end_date = dt.datetime(2022, 3, 27)

    url_provider_factory = UrlProviderFactory(url_type='endpoint', endpoint='PL_WYK_KSE/data')
    extractor = PseExtractor(
        start_date=period_start_date,
        end_date=period_end_date,
        url_provider_factory=url_provider_factory,
    )
    _system_march = extractor.extract()
    system_march = _system_march[period_end_date]

    url_provider_factory = UrlProviderFactory(url_type='endpoint', endpoint='PL_GEN_MOC_JW_EPS/data')
    extractor = PseExtractor(
        start_date=period_start_date,
        end_date=period_end_date,
        url_provider_factory=url_provider_factory,
    )
    _units_march = extractor.extract()
    units_march = _units_march[period_end_date]



    period_start_date = dt.datetime(2022, 10, 29)
    period_end_date = dt.datetime(2022, 10, 30)

    url_provider_factory = UrlProviderFactory(url_type='endpoint', endpoint='PL_WYK_KSE/data')
    extractor = PseExtractor(
        start_date=period_start_date,
        end_date=period_end_date,
        url_provider_factory=url_provider_factory,
    )
    _system_october = extractor.extract()
    system_october = _system_october[period_end_date]

    url_provider_factory = UrlProviderFactory(url_type='endpoint', endpoint='PL_GEN_MOC_JW_EPS/data')
    extractor = PseExtractor(
        start_date=period_start_date,
        end_date=period_end_date,
        url_provider_factory=url_provider_factory,
    )
    _units_october = extractor.extract()
    units_october = _units_october[period_end_date]
