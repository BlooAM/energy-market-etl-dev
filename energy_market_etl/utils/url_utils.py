import datetime as dt
from typing import Callable, Optional

from energy_market_etl.utils.date_utils import extract_date_components


class UrlProviderFactory:
    def __init__(self, url_type: str) -> None:
        self.url_type = url_type

    @staticmethod
    def endpoint_url_provider(date: dt.datetime, url_base: str, endpoint: str) -> str:
        year, month, day = extract_date_components(date)
        return f'{url_base}/{endpoint}/{year}{month:02d}{day:02d}'

    @staticmethod
    def parametrized_url_provider(date: dt.datetime, url_base: str, endpoint: str, parameter_name: str) -> str:
        year, month, day = extract_date_components(date)
        return f'{url_base}/{endpoint}?{parameter_name}={day:02d}-{month:02d}-{year}'

    def get_url_provider(self, url_base: str, endpoint: str, parameter_name: Optional[str] = '') -> Callable:
        if self.url_type == 'endpoint':
            return lambda date: UrlProviderFactory.endpoint_url_provider(
                date=date,
                url_base=url_base,
                endpoint=endpoint,
            )
        elif self.url_type == 'parametrized':
            return lambda date: UrlProviderFactory.parametrized_url_provider(
                date=date,
                url_base=url_base,
                endpoint=endpoint,
                parameter_name=parameter_name,
            )
        else:
            raise NotImplementedError('') #TODO: error message here
