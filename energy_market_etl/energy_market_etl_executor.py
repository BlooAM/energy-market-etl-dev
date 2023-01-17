from dataclasses import dataclass
import datetime as dt

import pydantic

from energy_market_etl.etl_executor import EtlExecutor
from energy_market_etl.etls.energy_market_etl import EnergyMarketEtl


_TODAY = dt.datetime.today()


class FutureDateError(Exception):
    def __int__(self, date: dt.datetime, message: str):
        self.date = date
        self.message = message
        super().__init__(message=self.message)


class NonChronologicalDateOrderError(Exception):
    def __int__(self, start_date: dt.datetime, end_date: dt.datetime, message: str):
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
    def end_date_validator(cls, attribute) -> None:
        if attribute > _TODAY:
            raise FutureDateError(
                date=attribute,
                message="" #TODO: add message for this exception
            )

    @pydantic.root_validator(pre=False)
    @classmethod
    def dates_chronological_order_validator(cls, attributes):
        start_date, end_date = attributes.get('start_date'), attributes.get('end_date')
        print(attributes)
        if start_date > end_date:
            raise NonChronologicalDateOrderError(
                start_date=start_date,
                end_date=end_date,
                message="", #TODO: add message for this exception
            )

        return attributes

    def execute(self):
        etl = self.__get_etl()

    def __get_etl(self) -> EnergyMarketEtl:
        return EnergyMarketEtl()


if __name__ == '__main__':
    start_date = dt.datetime(2020, 12, 15, 0, 0)
    end_date = dt.datetime(2020, 12, 21, 0, 0)
    future_date = dt.datetime(2024, 12, 21, 0, 0)
    data_source = ''
    EnergyMarketEtlExecutor(
        start_date=start_date,
        end_date=future_date,
        data_source=data_source,
    )
