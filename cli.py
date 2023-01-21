import argparse
import logging
import datetime as dt

from energy_market_etl.utils.dynamic_etl_loader import get_etl_keys
from energy_market_etl.etl_executor import EtlExecutor


def main():
    implemented_report_types = get_etl_keys()
    parser = argparse.ArgumentParser(description="Generate report regarding energy sector data")
    parser.add_argument(
        "-rt", "--report-type",
        type=str,
        help="Report type",
        required=True,
        choices=implemented_report_types
    )
    parser.add_argument(
        "-sd", "--start-date",
        type=lambda date: dt.datetime.strptime(date, "%Y-%m-%d"),
        help="First data snapshot date (YYYY-MM-DD format)",
        required=True
    )
    parser.add_argument(
        "-ed", "--end-date",
        type=lambda date: dt.datetime.strptime(date, "%Y-%m-%d"),
        help="Last data snapshot date (YYYY-MM-DD format)",
        required=True
    )
    # parser.add_argument(
    #     "-v", "--verbose",
    #     type=int,
    #     help="The date of vaccination",
    #     required=False
    # )
    args = parser.parse_args()

    for arg in [args.start_date, args.end_date, args.report_type]:
        print(f'Argument={arg} /// Type={type(arg)}')

    etl_executor = EtlExecutor(
        start_date=args.start_date,
        end_date=args.end_date,
        report_type=args.report_type,
    )
    etl_executor.execute()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.exception(e)
