import datetime as dt

from energy_market_etl.etl_executor import EtlExecutor
from energy_market_etl.etls.system_data_etl import SystemDataEtl
from energy_market_etl.etls.market_data_etl import MarketDataEtl

if __name__ == '__main__':
    period_start_date = dt.datetime(2023, 1, 1)
    period_end_date = dt.datetime(2023, 1, 5)
    report_type = 'market_data' #['market_data', 'system_basic_data', 'system_units_data']

    etl = MarketDataEtl(
        start_date=period_start_date,
        end_date=period_end_date,
        report_type=report_type,
    )
    etl.extract()
    etl.transform()
    etl.load()

    extracted_data = etl._MarketDataEtl__extracted_data
    transformed_data = etl._MarketDataEtl__transformed_data
