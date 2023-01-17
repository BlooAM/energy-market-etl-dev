import datetime as dt

from energy_market_etl.energy_market_etl_executor import EnergyMarketEtlExecutor


if __name__ == '__main__':
    period_start_date = dt.datetime(2022, 12, 16)
    period_end_date = dt.datetime(2022, 12, 28)
    data_source = ''

    etl_executor = EnergyMarketEtlExecutor(
        start_date=period_start_date,
        end_date=period_end_date,
        data_source=data_source,
    )
    etl_executor.execute()
