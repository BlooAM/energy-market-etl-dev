import datetime as dt
from itertools import chain, repeat
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
    tables = bs.findAll('table', {'id': 'footable_kontrakty_godzinowe'})
    if len(tables) != 1:
        raise FileNotFoundError('')
    else:
        table = tables[0]

        table_head = table.thead
        table_head_rows = table_head.findAll('tr')
        assert len(table_head_rows) == 2

        titles_row = table_head_rows[0]
        title_tags = [title_tag for title_tag in titles_row.findAll('th')]
        title_widths = [int(title_tag.get('colspan')) if title_tag.get('colspan') else 1 for title_tag in title_tags]
        title_lists = [list(repeat(title.text, n_copies)) for (title, n_copies) in zip(title_tags, title_widths)]
        titles = list(chain(*title_lists))

        subtitles_row = table_head_rows[1]
        subtitles = [subtitle.text for subtitle in subtitles_row.findAll('th')]

        assert len(titles) == len(subtitles)

        column_names = [f'{title}, {subtitle}' for (title, subtitle) in zip(titles, subtitles)] #TODO: fix first column name...


        table_content = table.tbody
