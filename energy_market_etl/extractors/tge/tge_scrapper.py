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
from dateutil.relativedelta import relativedelta
import pandas as pd
from retry import retry

from energy_market_etl.extractors.tge.utils import _TGE_DATA_TYPE_URL_MAPPER
from energy_market_etl.utils.date_utils import FLOAT_REGEXP


_HTTP_REQUEST_RETRY_DELAY_TIME = 30
_HTTP_REQUEST_RETRY_ATTEMPTS = 5


def _row_cell_formatter(row_value):
    row_value_formatted = row_value.strip().replace(',', '.')
    return float(row_value_formatted) if FLOAT_REGEXP(row_value_formatted) else None


class TgeScrapper:
    _RDN_TABLE_ID = 'footable_kontrakty_godzinowe'
    _RETENTION_HORIZON_MONTHS = 2

    def __init__(self, data_type: str) -> None:
        self.data_type = data_type #TODO: parse data_type -> not pydantic!
        self.__url_getter = _TGE_DATA_TYPE_URL_MAPPER.get(self.data_type)

    def scrape(self, date: dt.datetime) -> pd.DataFrame:
        today = dt.datetime.today()
        last_available_data_snapshot_date = \
            today - relativedelta(months=TgeScrapper._RETENTION_HORIZON_MONTHS) + relativedelta(days=1)
        if date < last_available_data_snapshot_date:
            logging.warning(f'TGE data not available for date: {date} due to retention policy')
            return pd.DataFrame()
        else:
            try:
                html_parser = self.__get_html_parser(date=date)
            except HTTPError as e:
                print(e)    #TODO: raise custom error
                return pd.DataFrame()
            except URLError as e:
                print(e)    #TODO: raise custom error
                return pd.DataFrame()
            tables = html_parser.findAll('table', {'id': TgeScrapper._RDN_TABLE_ID})
            if len(tables) != 1:
                raise AttributeError(f'Table does not exist for date: {date}')
            else:
                table = tables[0]
                table_head_data = TgeScrapper._parse_table_metadata(table.thead) #TODO: check if table has attr thead
                table_body_data = TgeScrapper._parse_table_data(table.tbody) #TODO: check if table has attr tbody
                table_summary_data = TgeScrapper._parse_table_data(table.tfoot) #TODO: check if table has attr tfoot
                data = [*table_body_data, *table_summary_data]
                data_snapshot = pd.DataFrame(data, columns=table_head_data)
                return data_snapshot

    @retry(IncompleteRead, delay=_HTTP_REQUEST_RETRY_DELAY_TIME, tries=_HTTP_REQUEST_RETRY_ATTEMPTS) #TODO: ???
    def __get_html_parser(self, date: dt.datetime) -> BeautifulSoup:
        url = self.__url_getter(date)
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


if __name__ == '__main__':
    start_date = dt.datetime(2022, 12, 15)
    end_date = dt.datetime(2023, 1, 3)
    future_date = dt.datetime(2024, 1, 1)
    past_date = dt.datetime(2020, 1, 1)
    data_type = 'rdn_data'
    df = TgeScrapper(data_type=data_type).scrape(date=start_date)


