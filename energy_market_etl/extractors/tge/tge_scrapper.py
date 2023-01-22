import datetime as dt
import http.client
import logging
from http.client import IncompleteRead
from itertools import chain, repeat
from typing import List, Union
from urllib.request import urlopen
from urllib.error import HTTPError, URLError

import bs4
from bs4 import BeautifulSoup
import pandas as pd
from retry import retry

from energy_market_etl.utils.date_utils import FLOAT_REGEXP


_HTTP_REQUEST_RETRY_DELAY_TIME = 30
_HTTP_REQUEST_RETRY_ATTEMPTS = 5


def _row_cell_formatter(row_value):
    row_value_formatted = row_value.strip().replace(',', '.')
    return float(row_value_formatted) if FLOAT_REGEXP(row_value_formatted) else None


class TgeScrapper:
    def __init__(self, table_id: str) -> None:
        self.table_id = table_id

    def scrape(self, url: str) -> pd.DataFrame:
        try:
            html_parser = self.__get_html_parser(url=url)
        except HTTPError as e:
            print(e)    #TODO: raise custom error
            return pd.DataFrame()
        except URLError as e:
            print(e)    #TODO: raise custom error
            return pd.DataFrame()
        tables = html_parser.findAll('table', {'id': self.table_id})
        if len(tables) != 1:
            raise AttributeError(f'Table does not exist for date')
        else:
            table = tables[0]
            table_head_data = TgeScrapper._parse_table_metadata(table.thead) #TODO: check if table has attr thead
            table_body_data = TgeScrapper._parse_table_data(table.tbody) #TODO: check if table has attr tbody
            table_summary_data = TgeScrapper._parse_table_data(table.tfoot) #TODO: check if table has attr tfoot
            data = [*table_body_data, *table_summary_data]
            data_snapshot = pd.DataFrame(data, columns=table_head_data)
            return data_snapshot

    @retry(IncompleteRead, delay=_HTTP_REQUEST_RETRY_DELAY_TIME, tries=_HTTP_REQUEST_RETRY_ATTEMPTS) #TODO: ???
    def __get_html_parser(self, url: str) -> BeautifulSoup:
        html = urlopen(url)
        html_parser = BeautifulSoup(html.read(), 'html.parser')
        return html_parser

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
