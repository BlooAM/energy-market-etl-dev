import datetime as dt

from energy_market_etl.etl_executor import EtlExecutor

import pydantic


if __name__ == '__main__':
    period_start_date = dt.datetime(2022, 12, 16)
    period_end_date = dt.datetime(2022, 12, 28)
    report_type = 'market_data' #['market_data', 'system_basic_data', 'system_units_data']

    etl_executor = EtlExecutor(
        start_date=period_start_date,
        end_date=period_end_date,
        report_type=report_type,
    )
    etl_executor.execute()
