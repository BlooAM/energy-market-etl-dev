import collections
import datetime as dt
from typing import Dict, Iterable, List, Union

import pandas as pd

from energy_market_etl.utils.class_metadata_utils import class_names
from energy_market_etl.extractors.extractor import Extractor
from energy_market_etl.transformers.transformer import Transformer
from energy_market_etl.loaders.loader import Loader
from energy_market_etl.extractors.tge.tge_extractor import TgeExtractor
from energy_market_etl.transformers.date_column.date_column_transformer import DateColumnTransformer
from energy_market_etl.transformers.vertical_stack.vertical_stack_transformer import VerticalStackTransformer
from energy_market_etl.loaders.csv.csv_loader import CsvLoader
from energy_market_etl.etls.etl import Etl


class MarketDataEtl(Etl):
    ETL_KEYS: List[str] = [
        'market_data'
    ]

    def __init__(
            self,
            start_date: dt.datetime,
            end_date: dt.datetime,
            data_source: str,
            report_name: str,
    ) -> None:
        self.start_date = start_date
        self.end_date = end_date
        self.data_source = data_source
        self.report_name = f'{report_name}_{start_date.date()}_{end_date.date()}'
        self.__extracted_data: Dict[dt.datetime, pd.DataFrame] = {}
        self.__transformed_data: pd.DataFrame = pd.DataFrame()

    def extract(self) -> None:
        extract_layer: Extractor = TgeExtractor(
            start_date=self.start_date,
            end_date=self.end_date,
            data_type=self.data_source,
        )
        self.__extracted_data = extract_layer.extract()

    def transform(self) -> None:
        transform_layer: Iterable[Transformer] = [
            DateColumnTransformer(date_column_name='Data'), #TODO: argument spec in other place
            VerticalStackTransformer()
        ]
        self.__transformed_data = self.__extracted_data.copy()
        for transformer in transform_layer:
            self.__transformed_data = transformer.transform(self.__transformed_data)

    def load(self) -> None:
        load_layer: Loader = CsvLoader(
            file_name=self.report_name
        )
        load_layer.load(self.__transformed_data)


__all__ = class_names(MarketDataEtl)
