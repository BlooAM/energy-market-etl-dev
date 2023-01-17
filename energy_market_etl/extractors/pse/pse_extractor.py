import datetime as dt

from energy_market_etl.extractors.extractor import Extractor
from energy_market_etl.extractors.pse.utils import _PSE_DATA_TYPE_URL_MAPPER


class PseExtractor(Extractor):
    def __int__(self, start_date: dt.datetime, end_date: dt.datetime, data_type: str):
        self.start_date = start_date
        self.end_date = end_date
        self.__url_getter = _PSE_DATA_TYPE_URL_MAPPER.get(data_type)
        if self.__url_getter is None:
            raise NotImplementedError(f'data type: {data_type} not implemented') #TODO: replace with custom Exception

    def extract(self):
        pass

    def

