from typing import Tuple
import datetime as dt

from energy_market_etl.utils.date_utils import extract_date_components

TGE_REQUEST_GET_BASIC_URL = 'https://tge.pl/energia-elektryczna-rdn'
TGE_RDN_URL_PREFIX = 'dateShow'


def __get_tge_rdn_url(date: dt.datetime) -> str:
    year, month, day = extract_date_components(date)
    return f'{TGE_REQUEST_GET_BASIC_URL}?{TGE_RDN_URL_PREFIX}={day:02d}-{month:02d}-{year}'


_TGE_DATA_TYPE_URL_MAPPER = {
    'rdn_data': __get_tge_rdn_url,
}
