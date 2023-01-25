import datetime as dt
from pathlib import Path
from unittest.mock import patch

from bs4 import BeautifulSoup
import pandas as pd
import pytest

from energy_market_etl.extractors.tge.tge_extractor import TgeScrapper
from tests.unit.energy_market_etl.resources_path_getter import get_resources_path


def get_scrapper_resource_path(endpoint: str, date: dt.datetime, extension: str) -> Path:
    return get_resources_path() / 'extractors' / 'tge' / f'{endpoint}_{date.strftime("%Y%m%d")}.{extension}'


@pytest.mark.parametrize("endpoint", ['energia-elektryczna-rdn'])
def test_extract(start_date: dt.datetime, endpoint: str):
    scrapper = TgeScrapper(table_id='footable_kontrakty_godzinowe')

    url = get_scrapper_resource_path(endpoint=endpoint, date=start_date, extension='html')
    with open(url) as resource_reference:
        html_parser = BeautifulSoup(resource_reference, 'html.parser')

    with patch('energy_market_etl.extractors.tge.tge_scrapper.TgeScrapper.get_html_parser') as mock:
        mock.return_value = html_parser
        result = scrapper.scrape(url=url)

    resource_path = get_scrapper_resource_path(endpoint=endpoint, date=start_date, extension='csv')
    expected = pd.read_csv(resource_path, encoding='cp1250', sep=';')
    assert isinstance(result, pd.DataFrame)
    assert set(expected.columns[1:]) == set(result.columns[1:])
