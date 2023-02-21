from http.client import IncompleteRead
from itertools import chain, repeat
import logging
import requests
from requests.exceptions import HTTPError, Timeout, RequestException
from typing import List, Union


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
    def __init__(self, table_ids: str) -> None:
        self.table_ids = table_ids
        if len(self.table_ids) == 1:
            self.expected_structure = {
                'expected_no_head_rows': 2,
                'expected_no_columns': 7,
            }
        else:
            self.expected_structure = {
                'expected_no_head_rows': 1,
                'expected_no_columns': 6,
            }

    def scrape(self, url: str) -> pd.DataFrame:
        try:
            html_parser = self.get_html_parser(url=url)
        except RequestException as e:
            logging.warning(f'Exception has occured while scrapping with the following message: {e}')
            return pd.DataFrame()

        if len(self.table_ids) == 1:
            table_id = self.table_ids[0]
            # TgeScrapper._parse_hourly_data_table(html_parser=html_parser, table_id=table_id)
            tables = html_parser.findAll('table', {'id': table_id})
            if len(tables) != 1:
                raise TableNotFoundError(
                    message=f'Table with id {table_id} does not exist'
                )
            else:
                table = tables[0]
                table_head_data = self._parse_table_metadata(table.thead)
                table_body_data = self._parse_table_data(table.tbody)
                table_summary_data = self._parse_table_data(table.tfoot)
                data = [*table_body_data, *table_summary_data]
                data_snapshot = pd.DataFrame(data, columns=table_head_data)
                return data_snapshot
        else:
            data_snapshots_list = list()
            for table_id in self.table_ids:
                tables = html_parser.findAll('table', {'id': table_id})
                if len(tables) != 1:
                    raise TableNotFoundError(
                        message=f'Table with id {table_id} does not exist'
                    )
                else:
                    table = tables[0]
                    table_head_data = self._parse_table_metadata(table.thead)
                    table_body_data = self._parse_table_data(table.tbody)
                    data = [*table_body_data]
                    data_snapshots_list.append(pd.DataFrame(data, columns=table_head_data))

            data_snapshots = pd.concat(data_snapshots_list)
            data_snapshots = data_snapshots.dropna(axis=1, how='all')
            return data_snapshots

    @retry(
        exceptions=(IncompleteRead, HTTPError, Timeout),
        delay=_HTTP_REQUEST_RETRY_DELAY_TIME,
        tries=_HTTP_REQUEST_RETRY_ATTEMPTS,
    )
    def get_html_parser(self, url: str) -> BeautifulSoup:
        response = requests.get(url)
        html_parser = BeautifulSoup(response.text, "html.parser")
        return html_parser

    def _parse_row_data(self, row: bs4.element.Tag) -> List[Union[str, float, None]]:
        row_cells = row.findAll('td')
        expected_no_cells = self.expected_structure['expected_no_columns']
        if len(row_cells) != expected_no_cells:
            raise InvalidTableStructure(
                message=f'Invalid structure - TGE table should consist of {expected_no_cells} columns, '
                        f'instead found {len(row_cells)}'
            )
        index_row_value = row_cells[0].text
        numeric_row_values = [_row_cell_formatter(row.text) for row in row_cells[1:]]
        return [index_row_value, *numeric_row_values]

    def _parse_table_data(self, raw_table_content: bs4.element.Tag) -> List[List[Union[str, float, None]]]:
        raw_rows = raw_table_content.findAll('tr')
        parsed_rows = [self._parse_row_data(row) for row in raw_rows]
        return parsed_rows

    def _parse_table_metadata(self, raw_table_content: bs4.element.Tag) -> List[str]:
        table_head_rows = raw_table_content.findAll('tr')
        expected_no_head_rows = self.expected_structure['expected_no_head_rows']
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

        if len(table_head_rows) > 1:
            subtitles_row = table_head_rows[1]
            subtitles = [subtitle.text for subtitle in subtitles_row.findAll('th')]
            parsed_rows = [f'{title}, {subtitle}' for (title, subtitle) in zip(titles, subtitles)]
        else:
            parsed_rows = titles

        return parsed_rows


if __name__ == '__main__':
    index_data = True
    url = 'https://tge.pl/energia-elektryczna-rdn'

    table_ids = ['footable_indeksy_0', 'footable_indeksy_1'] if index_data else ['footable_kontrakty_godzinowe']
    scrapper = TgeScrapper(table_ids=table_ids)
    data = scrapper.scrape(url=url)
