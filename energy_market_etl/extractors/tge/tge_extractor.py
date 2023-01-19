import datetime as dt
from typing import Dict

import pandas as pd

from energy_market_etl.extractors.extractor import Extractor
from energy_market_etl.extractors.tge.tge_scrapper import TgeScrapper


class TgeExtractor(Extractor):
    def __init__(self, start_date: dt.datetime, end_date: dt.datetime, data_type: str):
        self.start_date = start_date
        self.end_date = end_date
        self.__scrapper = TgeScrapper(data_type=data_type)

    def extract(self) -> Dict[dt.datetime, pd.DataFrame]:
        data_snapshots = {}
        for date in pd.date_range(self.start_date, self.end_date):
            data_snapshots[date]: pd.DataFrame = self.__scrapper.scrape(date=date) #TODO: handle possible exceptions here

        return data_snapshots


if __name__ == '__main__':
    _start_date = dt.datetime(2022, 11, 30)
    _end_date = dt.datetime(2022, 12, 6)
    _data_type = 'rdn_data'

    extractor = TgeExtractor(
        start_date=_start_date,
        end_date=_end_date,
        data_type=_data_type,
    )
    data = extractor.extract()
    key_sample = list(data.keys())[0]
    data_sample = data.get(key_sample)
