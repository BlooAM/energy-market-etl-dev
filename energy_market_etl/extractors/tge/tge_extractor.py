import datetime as dt
import logging
from typing import Callable, Dict

from dateutil.relativedelta import relativedelta
import pandas as pd

from energy_market_etl.extractors.extractor import Extractor
from energy_market_etl.extractors.tge.tge_scrapper import TgeScrapper
from energy_market_etl.utils.url_utils import UrlProviderFactory


class TgeExtractor(Extractor):
    _TGE_REQUEST_URL_BASE = 'https://tge.pl/energia-elektryczna-rdn'
    _RETENTION_HORIZON_MONTHS = 2

    def __init__(self, start_date: dt.datetime, end_date: dt.datetime, data_access_endpoint: str):
        self.start_date = start_date
        self.end_date = end_date
        self.data_access_endpoint = data_access_endpoint
        self.__url_provider_factory = UrlProviderFactory(url_type='parametrized')
        self.__scrapper = TgeScrapper(table_id='footable_kontrakty_godzinowe') #TODO: dynamic table_id (via constructor?)

    def extract(self) -> Dict[dt.datetime, pd.DataFrame]:
        url_provider: Callable = self.__get_url_provider()

        data_snapshots = {}
        for date in pd.date_range(self.start_date, self.end_date):
            if TgeExtractor.is_data_available(date=date):
                url = url_provider(date=date)
                data_snapshots[date] = self.__scrapper.scrape(url=url)  # TODO: handle possible exceptions here
            else:
                logging.warning(f'TGE data not available for date: {date} due to retention policy')

        return data_snapshots

    def __get_url_provider(self) -> Callable:
        url_provider = self.__url_provider_factory.get_url_provider(
            url_base=TgeExtractor._TGE_REQUEST_URL_BASE,
            endpoint=self.data_access_endpoint,
            parameter_name='dateShow'
        )
        return url_provider

    @staticmethod
    def is_data_available(date: dt.datetime) -> bool:
        today = dt.datetime.today()
        last_available_data_snapshot_date = \
            today - relativedelta(months=TgeExtractor._RETENTION_HORIZON_MONTHS) + relativedelta(days=1)
        return date >= last_available_data_snapshot_date
