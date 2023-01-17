from typing import Tuple
import datetime as dt


PSE_CSV_GET_BASIC_URL = 'https://pse.pl/getcsv/-/export/csv'

PSE_UNITS_GENERATION_URL_PREFIX = 'PL_GEN_MOC_JW_EPS/data'
PSE_UNITS_GENERATION_URL_SUFFIX = 'unit/all'

PSE_SYSTEM_DATA_URL_PREFIX = 'PL_WYK_KSE/data'


def __extract_date_components(date: dt.datetime) -> Tuple[int]:
    year = date.year
    month = date.month
    day = date.day
    return year, month, day


def __get_pse_unit_generation_url(date: dt.datetime) -> str:
    year, month, day = __extract_date_components(date)
    return f'{PSE_CSV_GET_BASIC_URL}/{PSE_UNITS_GENERATION_URL_PREFIX}/{year}{month:02d}{day:02d}/' \
           f'{PSE_UNITS_GENERATION_URL_SUFFIX}'


def __get_pse_system_data_url(date: dt.datetime) -> str:
    year, month, day = __extract_date_components(date)
    return f'{PSE_CSV_GET_BASIC_URL}/{PSE_SYSTEM_DATA_URL_PREFIX}/{year}{month:02d}{day:02d}'


_PSE_DATA_TYPE_URL_MAPPER = {
    'system_data': __get_pse_system_data_url,
    'unit_generation_data': __get_pse_unit_generation_url,
}
