import datetime as dt
from typing import Any

import pydantic

from energy_market_etl.etls.etl import Etl
from energy_market_etl.utils.dynamic_etl_loader import get_etls, get_etl_keys


_TODAY = dt.datetime.today()


class FutureDateError(Exception):
    def __init__(self, date: dt.datetime, message: str) -> None:
        self.date = date
        self.message = message
        super().__init__(self.message)


class ReportTypeNotImplementedError(Exception):
    def __init__(self, report_type: dt.datetime, message: str) -> None:
        self.report_type = report_type
        self.message = message
        super().__init__(self.message)


class NonChronologicalDateOrderError(Exception):
    def __init__(self, start_date: dt.datetime, end_date: dt.datetime, message: str) -> None:
        self.start_date = start_date
        self.end_date = end_date
        self.message = message
        super().__init__(self.message)


class EtlExecutor(pydantic.BaseModel):
    start_date: dt.datetime
    end_date: dt.datetime
    report_type: str

    @pydantic.validator("end_date")
    @classmethod
    def end_date_validator(cls, value: dt.datetime) -> dt.datetime:
        if value > _TODAY:
            raise FutureDateError(
                date=value,
                message=f"given `end_date`={value} cannot be later than the current date"
            )
        return value


    @pydantic.validator("report_type")
    @classmethod
    def report_type_validator(cls, value: str) -> str:
        if value not in get_etl_keys():
            raise ReportTypeNotImplementedError(
                report_type=value,
                message=f"given `report_type`={value} is not implemented"
            )
        return value

    @pydantic.root_validator(pre=True)
    @classmethod
    def dates_chronological_order_validator(cls, values: Any) -> Any:
        start_date, end_date = values.get('start_date'), values.get('end_date')
        if start_date > end_date:
            raise NonChronologicalDateOrderError(
                start_date=start_date,
                end_date=end_date,
                message=f"`start_date`={start_date} and `end_date`={end_date} must be in chronological order",
            )
        return values

    def execute(self):
        etl = self.__get_etl()
        # etl.extract() #TODO: add loggs between layers
        # etl.transform() #TODO: add loggs between layers
        # etl.load() #TODO: add loggs between layers

        return etl #TODO: remove this line after tests

    def __get_etl(self) -> Etl:
        for EtlClass in get_etls():
            if self.report_type in EtlClass.ETL_KEYS:
                return EtlClass(
                    start_date=self.start_date,
                    end_date=self.end_date,
                    report_type=self.report_type,
                )


if __name__ == '__main__':
    start_date = dt.datetime(2022, 12, 15)
    end_date = dt.datetime(2022, 12, 30)
    end_date_ = dt.datetime(2022, 11, 13)
    future_date = dt.datetime(2024, 1, 1)
    report_type = 'system_data'

    etl_executor = EtlExecutor(
        start_date=start_date,
        end_date=end_date,
        report_type=report_type,
    )
    etl = etl_executor.execute()

    extracted_data = etl._SystemDataEtl__extracted_data
    key_sample = list(extracted_data.keys())[0]
    data_sample = extracted_data.get(key_sample)

    transformed_data = etl._SystemDataEtl__transformed_data
