import datetime as dt
from itertools import chain, repeat
from typing import List, Union
from urllib.request import urlopen

import bs4
import pandas as pd
from bs4 import BeautifulSoup

from energy_market_etl.extractors.tge.utils import _TGE_DATA_TYPE_URL_MAPPER
from energy_market_etl.utils.date_utils import FLOAT_REGEXP


def _row_value_formatter(row_value):
    row_value_formatted = row_value.strip().replace(',', '.')
    return float(row_value_formatted) if FLOAT_REGEXP(row_value_formatted) else None


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

        # 1 TABLE HEAD
        table_head = table.thead
        table_head_rows = table_head.findAll('tr')
        assert len(table_head_rows) == 2 #TODO: remove asserts

        titles_row = table_head_rows[0]
        title_tags = [title_tag for title_tag in titles_row.findAll('th')]
        title_widths = [int(title_tag.get('colspan')) if title_tag.get('colspan') else 1 for title_tag in title_tags]
        title_lists = [list(repeat(title.text, n_copies)) for (title, n_copies) in zip(title_tags, title_widths)]
        titles = list(chain(*title_lists))

        subtitles_row = table_head_rows[1]
        subtitles = [subtitle.text for subtitle in subtitles_row.findAll('th')]

        assert len(titles) == len(subtitles) #TODO: remove asserts

        column_names = [f'{title}, {subtitle}' for (title, subtitle) in zip(titles, subtitles)] #TODO: fix first column name...

        def scrape_row_data(row: bs4.element.Tag) -> List[Union[str, float, None]]:
            row_cells = row.findAll('td')
            assert len(row_cells) == 7 #TODO: remove asserts
            index_row_value = row_cells[0].text
            numeric_row_values = [_row_value_formatter(row.text) for row in row_cells[1:]]
            return [index_row_value, *numeric_row_values]

        # 2 TABLE BODY
        table_content = table.tbody
        retail_rows = table_content.findAll('tr')
        assert len(retail_rows) == 24 #TODO: remove asserts
        retail_data = [scrape_row_data(row) for row in retail_rows]

        # 3 TABLE FOOT
        table_foot = table.tfoot
        summary_rows = table_foot.findAll('tr')
        assert len(summary_rows) == 3  # TODO: remove asserts
        summary_data = [scrape_row_data(row) for row in summary_rows]

        data = [*retail_data, *summary_data]
        df = pd.DataFrame(data, columns=column_names)
