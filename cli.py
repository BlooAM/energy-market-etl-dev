import argparse
import logging
import datetime as dt

import coloredlogs

from energy_market_etl.utils.dynamic_etl_loader import get_etl_keys
from energy_market_etl.etl_executor import EtlExecutor, FutureDateError, NonChronologicalDateOrderError, \
    ReportTypeNotImplementedError


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
    args = parser.parse_args()

    coloredlogs.install(
        fmt='%(asctime)s - %(levelname)s - %(message)s',
        level='INFO',
        datefmt='%Y-%m-%d %H:%M:%S',
    )
    etl_executor = EtlExecutor(
        start_date=args.start_date,
        end_date=args.end_date,
        report_type=args.report_type,
    )
    etl_executor.execute()


if __name__ == "__main__":
    try:
        main()
    except FutureDateError as e:
        logging.error(e)
    except ReportTypeNotImplementedError as e:
        logging.error(e)
    except NonChronologicalDateOrderError as e:
        logging.error(e)
    except Exception as e:
        logging.exception(f'Program has stopped. An error has occured with the followin message\n: "{e}"')
