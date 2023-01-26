import coloredlogs
import datetime as dt

from energy_market_etl.etl_executor import EtlExecutor


if __name__ == '__main__':
    coloredlogs.install(
        fmt='%(asctime)s - %(levelname)s - %(message)s',
        level='DEBUG',
        datefmt='%Y-%m-%d %H:%M:%S',
    )

    start_date = dt.datetime(2022, 10, 1)
    end_date = dt.datetime(2023, 1, 20)

    etl_executor = EtlExecutor(
        start_date=start_date,
        end_date=end_date,
        report_type='market_data',
    )
    # etl = etl_executor.execute()