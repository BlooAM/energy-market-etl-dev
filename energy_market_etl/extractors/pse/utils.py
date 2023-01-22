import datetime as dt

from energy_market_etl.utils.date_utils import extract_date_components


PSE_CSV_GET_BASIC_URL = 'https://pse.pl/getcsv/-/export/csv'

PSE_UNITS_GENERATION_URL_PREFIX = 'PL_GEN_MOC_JW_EPS/data'
PSE_SYSTEM_DATA_URL_PREFIX = 'PL_WYK_KSE/data'


def __get_pse_unit_generation_url(date: dt.datetime) -> str:
    year, month, day = extract_date_components(date)
    return f'{PSE_CSV_GET_BASIC_URL}/{PSE_UNITS_GENERATION_URL_PREFIX}/{year}{month:02d}{day:02d}'


def __get_pse_system_data_url(date: dt.datetime) -> str:
    year, month, day = extract_date_components(date)
    return f'{PSE_CSV_GET_BASIC_URL}/{PSE_SYSTEM_DATA_URL_PREFIX}/{year}{month:02d}{day:02d}'


_PSE_DATA_TYPE_URL_MAPPER = { #TODO: keys in this dict must match Etl.ETL_KEYS
    'system_data': __get_pse_system_data_url,
    'system_units_data': __get_pse_unit_generation_url,
}
