import datetime as dt
from urllib.request import urlopen

from bs4 import BeautifulSoup

from energy_market_etl.extractors.tge.utils import _TGE_DATA_TYPE_URL_MAPPER


class TgeScrapper:
    def __int__(self, data_type: str) -> None:
        self.__url_getter = _TGE_DATA_TYPE_URL_MAPPER.get(data_type)



if __name__ == '__main__':
    start_date = dt.datetime(2022, 12, 15)
    end_date = dt.datetime(2023, 1, 3)
    future_date = dt.datetime(2024, 1, 1)
    past_date = dt.datetime(2020, 1, 1)
    data_type = 'rdn_data'

    url_getter = _TGE_DATA_TYPE_URL_MAPPER.get(data_type)
    url = url_getter(start_date)
    html = urlopen(url)

    bs = BeautifulSoup(html.read(), 'html.parser')
    tables = bs.findAll('table')
