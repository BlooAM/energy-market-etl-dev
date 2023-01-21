import datetime as dt
from typing import Dict, List

import pandas as pd

from energy_market_etl.utils.class_metadata_utils import class_names
from energy_market_etl.extractors.extractor import Extractor
from energy_market_etl.transformers.transformer import Transformer
from energy_market_etl.loaders.loader import Loader
from energy_market_etl.extractors.pse.pse_extractor import PseExtractor
from energy_market_etl.transformers.vertical_stack.vertical_stack_transformer import VerticalStackTransformer
from energy_market_etl.loaders.csv.csv_loader import CsvLoader
from energy_market_etl.etls.etl import Etl


class SystemDataEtl(Etl):
    ETL_KEYS: List[str] = [
        'system_data',
        'system_units_data'
    ]

    def __init__(
            self,
            start_date: dt.datetime,
            end_date: dt.datetime,
            report_type: str,
    ) -> None:
        self.start_date = start_date
        self.end_date = end_date
        self.data_source = report_type
        self.report_name = f'{report_type}_{start_date.date()}_{end_date.date()}'
        self.__extracted_data: Dict[dt.datetime, pd.DataFrame] = {}
        self.__transformed_data: pd.DataFrame = pd.DataFrame()

    def extract(self) -> None:
        extract_layer: Extractor = PseExtractor(
            start_date=self.start_date,
            end_date=self.end_date,
            data_type=self.data_source,
        )
        self.__extracted_data = extract_layer.extract()

    def transform(self) -> None:
        transform_layer: Transformer = VerticalStackTransformer()
        self.__transformed_data = self.__extracted_data.copy()
        self.__transformed_data = transform_layer.transform(self.__transformed_data)

    def load(self) -> None:
        load_layer: Loader = CsvLoader(
            file_name=self.report_name
        )
        load_layer.load(self.__transformed_data)


__all__ = class_names(SystemDataEtl)