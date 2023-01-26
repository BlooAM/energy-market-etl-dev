from http.client import IncompleteRead
from itertools import chain, repeat
import logging
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


class TableNotFoundError(Exception):
    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(self.message)


class InvalidTableStructure(Exception):
    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(self.message)


class TgeScrapper:
    def __init__(self, table_id: str) -> None:
        self.table_id = table_id

    def scrape(self, url: str) -> pd.DataFrame:
        try:
            html_parser = self.get_html_parser(url=url)
        except Exception as e:
            logging.warning(f'Error has occured while scrapping with the following message: {e}')

        tables = html_parser.findAll('table', {'id': self.table_id})
        if len(tables) != 1:
            raise TableNotFoundError(
                message=f'Table with id {self.table_id} does not exist'
            )
        else:
            table = tables[0]
            table_head_data = TgeScrapper._parse_table_metadata(table.thead)
            table_body_data = TgeScrapper._parse_table_data(table.tbody)
            table_summary_data = TgeScrapper._parse_table_data(table.tfoot)
            data = [*table_body_data, *table_summary_data]
            data_snapshot = pd.DataFrame(data, columns=table_head_data)
            return data_snapshot

    @retry(IncompleteRead, delay=_HTTP_REQUEST_RETRY_DELAY_TIME, tries=_HTTP_REQUEST_RETRY_ATTEMPTS)
    def get_html_parser(self, url: str) -> BeautifulSoup:
        try:
            html = urlopen(url)
        except HTTPError as e:
            logging.warning(f'HTTP Error has occured while scrapping with the following message: {e}')
        except URLError as e:
            logging.warning(f'URL Error has occured while scrapping with the following message: {e}')

        html_parser = BeautifulSoup(html.read(), 'html.parser')
        return html_parser

    @staticmethod
    def _parse_row_data(row: bs4.element.Tag) -> List[Union[str, float, None]]:
        row_cells = row.findAll('td')
        expected_no_cells = 7
        if len(row_cells) != expected_no_cells:
            raise InvalidTableStructure(
                message=f'Invalid structure - TGE table should consist of {expected_no_cells} columns, '
                        f'instead found {len(row_cells)}'
            )
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
        expected_no_head_rows = 2
        if len(table_head_rows) != expected_no_head_rows:
            raise InvalidTableStructure(
                message=f'Invalid structure - TGE table should consist of {expected_no_head_rows} head rows, '
                        f'instead found {len(table_head_rows)}'
            )

        titles_row = table_head_rows[0]
        title_tags = [title_tag for title_tag in titles_row.findAll('th')]
        title_widths = [int(title_tag.get('colspan')) if title_tag.get('colspan') else 1 for title_tag in title_tags]
        title_lists = [list(repeat(title.text, n_copies)) for (title, n_copies) in zip(title_tags, title_widths)]
        titles = list(chain(*title_lists))

        subtitles_row = table_head_rows[1]
        subtitles = [subtitle.text for subtitle in subtitles_row.findAll('th')]

        parsed_rows = [f'{title}, {subtitle}' for (title, subtitle) in zip(titles, subtitles)]
        return parsed_rows
