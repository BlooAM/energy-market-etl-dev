import datetime as dt
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest

from energy_market_etl.extractors.tge.tge_extractor import TgeScrapper
from tests.unit.energy_market_etl.resources_path_getter import get_resources_path


def get_scrapper_resource_path(endpoint: str, date: dt.datetime) -> Path:
    return get_resources_path() / 'extractors' / 'tge' / f'{endpoint}_{date.strftime("%Y%m%d")}.html'


@pytest.mark.parametrize("endpoint", ['energia-elektryczna-rdn'])
def test_units_extract(start_date: dt.datetime, end_date: dt.datetime, endpoint: str):
    mocked_url_factory_provider = MagicMock()
    mocked_url_factory_provider.get_url_provider.return_value = \
        lambda date: get_scrapper_resource_path(date=date, endpoint=endpoint)
    extractor = TgeScrapper(
        start_date=start_date,
        end_date=end_date,
        url_provider_factory=mocked_url_factory_provider
    )

    date_range = pd.date_range(start_date, end_date)
    resource_paths = [get_extractor_resource_path(endpoint, date) for date in date_range]
    expected = {
        date: pd.read_csv(path, encoding='cp1250', sep=';')
        for date, path in zip(date_range, resource_paths)
    }
    result = extractor.extract()

    for date in date_range:
        assert isinstance(result.get(date), pd.DataFrame)
        assert set(expected.get(date).columns) == set(result.get(date).columns)




# with open("energia-elektryczna-rdn_20230101.html") as fp:
#     soup = BeautifulSoup(fp, 'html.parser')