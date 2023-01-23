import datetime as dt
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd

from energy_market_etl.extractors.pse.pse_extractor import PseExtractor
from tests.unit.energy_market_etl.resources_path_getter import get_resources_path


def get_extractor_resource_path(endpoint: str, date: dt.datetime) -> Path:
    return get_resources_path() / 'extractors' / 'pse' / f'{endpoint}_{date.strftime("%Y%m%d")}.csv'


# def test_units_extract():
start_date = dt.datetime(2023, 1, 1)
end_date = dt.datetime(2023, 1, 3)

endpoint = 'PL_GEN_MOC_JW_EPS'

extractor = PseExtractor(
    start_date=start_date,
    end_date=end_date,
    data_access_endpoint=endpoint
)
mocked_url_factory_provider = MagicMock()
mocked_url_factory_provider.get_url_provider.return_value = \
    lambda date: get_extractor_resource_path(date=date, endpoint=endpoint)
extractor.UrlProviderFactory = mocked_url_factory_provider

date_range = pd.date_range(start_date, end_date)
resource_paths = [get_extractor_resource_path(endpoint, date) for date in date_range]
expected = {
    date: pd.read_csv(path, encoding='cp1250', sep=';')
    for date, path in zip(date_range, resource_paths)
}
result = extractor.extract()

    # assert expected == result

