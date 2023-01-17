from dataclasses import dataclass
import datetime as dt

from energy_market_etl.etl_executor import EtlExecutor
from energy_market_etl.etls.energy_market_etl import EnergyMarketEtl


class EnergyMarketEtlExecutor(EtlExecutor):
    def __int__(
            self,
            start_date: dt.datetime,
            end_date: dt.datetime,
            data_source: str,
    ):
        self.start_date = start_date
        self.end_date = end_date
        self.data_source = data_source
        self.etl = EnergyMarketEtl()

    def execute(self):
        pass
