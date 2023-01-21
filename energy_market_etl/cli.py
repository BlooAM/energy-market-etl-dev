import argparse
import logging

from energy_market_etl.utils.dynamic_etl_loader import get_etl_keys
from energy_market_etl.etl_executor import EtlExecutor


def main():
    implemented_report_types = get_etl_keys()
    parser = argparse.ArgumentParser(description="Generate report regarding energy sector data")
    parser.add_argument(
        "-rt", "--report-type",
        type=str,
        nargs="+",
        help="Report type",
        required=True,
        choices=implemented_report_types
    )
    parser.add_argument(
        "-sd", "--start-date",
        type=str,
        help="First data snapshot date (YYYY-MM-DD format)",
        required=True
    )
    parser.add_argument(
        "-ed", "--end-date",
        type=str,
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
