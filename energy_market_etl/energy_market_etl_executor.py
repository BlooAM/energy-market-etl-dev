from dataclasses import dataclass
import datetime as dt
from typing import Any

import pydantic

from energy_market_etl.etls.energy_market_etl import EnergyMarketEtl


_TODAY = dt.datetime.today()


class FutureDateError(Exception):
    def __int__(self, date: dt.datetime, message: str) -> None:
        self.date = date
        self.message = message
        super().__init__(message=self.message)


class NonChronologicalDateOrderError(Exception):
    def __int__(self, start_date: dt.datetime, end_date: dt.datetime, message: str) -> None:
        self.start_date = start_date
        self.end_date = end_date
        self.message = message
        super().__init__(message=self.message)


class EtlExecutor(pydantic.BaseModel): #TODO: inherits from abstract EtlExecutor?
    start_date: dt.datetime
    end_date: dt.datetime
    report_type: str #TODO: add validator for report type and mapper report_type->date_source

    @pydantic.validator("end_date")
    @classmethod
    def end_date_validator(cls, value: dt.datetime) -> None:
        if value > _TODAY:
            raise FutureDateError(
                value,
                "`end_date` cannot be later than the current date" #TODO: keyword args, not positional
            )

    @pydantic.root_validator(pre=True)
    @classmethod
    def dates_chronological_order_validator(cls, values: Any) -> Any:
        start_date, end_date = values.get('start_date'), values.get('end_date')
        if start_date > end_date:
            raise NonChronologicalDateOrderError(
                start_date,
                end_date,
                "`start_date` and `end_date` must be in chronological order", #TODO: keyword args, not positional
            )
        return values

    def execute(self):
        etl = self.__get_etl()
        # etl.extractor() #TODO: add loggs between layers
        # etl.transform() #TODO: add loggs between layers
        # etl.load() #TODO: add loggs between layers

    def __get_etl(self) -> EnergyMarketEtl:
        data_source = self.report_type
        return EnergyMarketEtl(
            start_date=self.start_date,
            end_date=self.end_date,
            data_source=data_source,
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
