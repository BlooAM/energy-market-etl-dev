import datetime as dt

from energy_market_etl.utils.date_utils import extract_date_components


class UrlProvider:
    def __init__(self, url_type: str) -> None:
        self.url_type = url_type

    @staticmethod
    def endpoint_url_provider(cls, date: dt.datetime, url_base: str, prefix: str):
        year, month, day = extract_date_components(date)
        return f'{url_base}/{prefix}/{year}{month:02d}{day:02d}'

    @staticmethod
    def parametrized_url_provider(cls, date: dt.datetime, url_base: str, prefix: str):
        year, month, day = extract_date_components(date)
        return f'{url_base}?{prefix}={day:02d}-{month:02d}-{year}'

    def get_url_provider(self):
        if self.url_type == 'endpoint':
            return UrlProvider.endpoint_url_provider
        else:
            return UrlProvider.parametrized_url_provider
