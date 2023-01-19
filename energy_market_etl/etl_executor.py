from dataclasses import dataclass
import datetime as dt
from typing import Any

import pydantic

from energy_market_etl.etls.etl import Etl
from energy_market_etl.etls.market_data_etl import MarketDataEtl

_REPORT_TYPES = [
    'market_data',
    'system_units_data',
] #TODO: move implemented report types to config file
_TODAY = dt.datetime.today()


class FutureDateError(Exception):
    def __init__(self, date: dt.datetime, message: str) -> None:
        self.date = date
        self.message = message
        super().__init__(message=self.message)


class ReportTypeNotImplementedError(Exception):
    def __init__(self, report_type: dt.datetime, message: str) -> None:
        self.report_type = report_type
        self.message = message
        super().__init__(message=self.message)


class NonChronologicalDateOrderError(Exception):
    def __init__(self, start_date: dt.datetime, end_date: dt.datetime, message: str) -> None:
        self.start_date = start_date
        self.end_date = end_date
        self.message = message
        super().__init__(message=self.message)


class EtlExecutor(pydantic.BaseModel):
    start_date: dt.datetime
    end_date: dt.datetime
    report_type: str

    @pydantic.validator("end_date")
    @classmethod
    def end_date_validator(cls, value: dt.datetime) -> None:
        if value > _TODAY:
            raise FutureDateError(
                date=value,
                message="`end_date` cannot be later than the current date"
            )

    @pydantic.validator("report_type")
    @classmethod
    def report_type_validator(cls, value: str) -> None:
        if value not in _REPORT_TYPES:
            raise ReportTypeNotImplementedError(
                report_type=value,
                message="`end_date` cannot be later than the current date"
            )

    @pydantic.root_validator(pre=True)
    @classmethod
    def dates_chronological_order_validator(cls, values: Any) -> Any:
        start_date, end_date = values.get('start_date'), values.get('end_date')
        if start_date > end_date:
            raise NonChronologicalDateOrderError(
                start_date=start_date,
                end_date=end_date,
                message="`start_date` and `end_date` must be in chronological order",
            )
        return values

    def execute(self):
        etl = self.__get_etl()
        # etl.extract() #TODO: add loggs between layers
        # etl.transform() #TODO: add loggs between layers
        # etl.load() #TODO: add loggs between layers

    def __get_etl(self) -> Etl:
        data_source = self.report_type #TODO: add mapper (report_type) -> (data_source, report_name)
        return MarketDataEtl( #TODO: add etl factory based on (data_source, report_name)
            start_date=self.start_date,
            end_date=self.end_date,
            data_source=data_source,
            report_name=self.report_type,
        )


if __name__ == '__main__':
    start_date = dt.datetime(2020, 11, 15)
    end_date = dt.datetime(2020, 12, 3)
    end_date_ = dt.datetime(2020, 11, 13)
    future_date = dt.datetime(2024, 1, 1)
    report_type = ''

    etl_executor = EtlExecutor(
        start_date=start_date,
        end_date=end_date,
        report_type=report_type,
    )
    etl_executor.execute()
