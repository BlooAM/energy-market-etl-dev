from dataclasses import dataclass
import datetime as dt
from typing import Any

import pydantic

from energy_market_etl.etl_executor import EtlExecutor
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


class EnergyMarketEtlExecutor(pydantic.BaseModel): #TODO: inherits from abstract EtlExecutor?
    start_date: dt.datetime
    end_date: dt.datetime
    data_source: str

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

    def __get_etl(self) -> EnergyMarketEtl:
        return EnergyMarketEtl()
