import datetime as dt
import http.client
from itertools import chain, repeat
from typing import List, Union
from urllib.request import urlopen
from urllib.error import HTTPError, URLError

import bs4
import pandas as pd
from bs4 import BeautifulSoup

from energy_market_etl.extractors.tge.utils import _TGE_DATA_TYPE_URL_MAPPER
from energy_market_etl.utils.date_utils import FLOAT_REGEXP


_RDN_TABLE_ID = 'footable_kontrakty_godzinowe'


def _row_cell_formatter(row_value):
    row_value_formatted = row_value.strip().replace(',', '.')
    return float(row_value_formatted) if FLOAT_REGEXP(row_value_formatted) else None


class TgeScrapper:
    def __init__(self, data_type: str) -> None:
        self.data_type = data_type #TODO: parse data_type -> not pydantic!

    def scrape(self, date: dt.datetime) -> pd.DataFrame:
        raw_html = self.__get_raw_html(date=date)
        html_parser = BeautifulSoup(raw_html.read(), 'html.parser')
        tables = html_parser.findAll('table', {'id': _RDN_TABLE_ID})
        if len(tables) != 1:
            raise AttributeError(f'Table does not exist for date: {date}') #TODO: message here
        else:
            table = tables[0]
            table_head_data = TgeScrapper._parse_table_metadata(table.thead) #TODO: check if table has attr thead
            table_body_data = TgeScrapper._parse_table_data(table.tbody) #TODO: check if table has attr tbody
            table_summary_data = TgeScrapper._parse_table_data(table.tfoot) #TODO: check if table has attr tfoot
            data = [*table_body_data, *table_summary_data]
            data_snapshot = pd.DataFrame(data, columns=table_head_data)
            return data_snapshot

    def __get_raw_html(self, date: dt.datetime) -> Union[http.client.HTTPResponse, None]:
        url_getter = _TGE_DATA_TYPE_URL_MAPPER.get(self.data_type)
        url = url_getter(date)
        try:
            html = urlopen(url)
        except HTTPError as e:
            print(e)    #TODO: raise custom error
            return  None
        except URLError as e:
            print(e)    #TODO: raise custom error
            return None
        return html

    @staticmethod
    def _parse_row_data(row: bs4.element.Tag) -> List[Union[str, float, None]]:
        row_cells = row.findAll('td')
        assert len(row_cells) == 7  # TODO: remove asserts
        index_row_value = row_cells[0].text
        numeric_row_values = [_row_cell_formatter(row.text) for row in row_cells[1:]]
        return [index_row_value, *numeric_row_values]

    @staticmethod
    def _parse_table_data(raw_table_content: bs4.element.Tag) -> List[List[Union[str, float, None]]]:
        raw_rows = raw_table_content.findAll('tr')
        parsed_rows = [TgeScrapper._parse_row_data(row) for row in raw_rows]
        return parsed_rows

    @staticmethod
    def _parse_table_metadata(raw_table_content: bs4.element.Tag) -> List[str]:
        table_head_rows = raw_table_content.findAll('tr')
        assert len(table_head_rows) == 2  # TODO: remove asserts
        titles_row = table_head_rows[0]
        title_tags = [title_tag for title_tag in titles_row.findAll('th')]
        title_widths = [int(title_tag.get('colspan')) if title_tag.get('colspan') else 1 for title_tag in
                        title_tags]
        title_lists = [list(repeat(title.text, n_copies)) for (title, n_copies) in zip(title_tags, title_widths)]
        titles = list(chain(*title_lists))
        subtitles_row = table_head_rows[1]
        subtitles = [subtitle.text for subtitle in subtitles_row.findAll('th')]
        assert len(titles) == len(subtitles)  # TODO: remove asserts
        parsed_rows = [f'{title}, {subtitle}' for (title, subtitle) in
                       zip(titles, subtitles)]  # TODO: fix first column name...
        return parsed_rows


if __name__ == '__main__':
    start_date = dt.datetime(2022, 12, 15)
    end_date = dt.datetime(2023, 1, 3)
    future_date = dt.datetime(2024, 1, 1)
    past_date = dt.datetime(2020, 1, 1)
    data_type = 'rdn_data'
    df = TgeScrapper(data_type=data_type).scrape(date=start_date)


