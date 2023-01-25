import datetime as dt
from typing import Callable, Optional

from energy_market_etl.utils.date_utils import extract_date_components


class UrlProviderFactory:
    def __init__(self, url_type: str, endpoint: str) -> None:
        self.url_type = url_type
        self.endpoint = endpoint

    def _endpoint_url_provider(self, date: dt.datetime, url_base: str) -> str:
        year, month, day = extract_date_components(date)
        return f'{url_base}/{self.endpoint}/{year}{month:02d}{day:02d}'

    def _parametrized_url_provider(self, date: dt.datetime, url_base: str, parameter_name: str) -> str:
        year, month, day = extract_date_components(date)
        return f'{url_base}/{self.endpoint}?{parameter_name}={day:02d}-{month:02d}-{year}'

    def get_url_provider(self, url_base: str, parameter_name: str = '') -> Callable:
        if self.url_type == 'endpoint':
            return lambda date: self._endpoint_url_provider(date=date, url_base=url_base)
        elif self.url_type == 'parametrized':
            return lambda date: self._parametrized_url_provider(
                date=date,
                url_base=url_base,
                parameter_name=parameter_name,
            )
        else:
            raise NotImplementedError('') #TODO: error message here
