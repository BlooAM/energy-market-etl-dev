import datetime as dt
from typing import Dict, Iterable

import pandas as pd

from energy_market_etl.utils.class_metadata_utils import class_names
from energy_market_etl.utils.url_utils import UrlProviderFactory
from energy_market_etl.extractors.extractor import Extractor
from energy_market_etl.transformers.transformer import Transformer
from energy_market_etl.loaders.loader import Loader
from energy_market_etl.extractors.tge.tge_extractor import TgeExtractor
from energy_market_etl.transformers.date_column.date_column_transformer import DateColumnTransformer
from energy_market_etl.transformers.stack.stack_transformer import StackTransformer
from energy_market_etl.loaders.csv.csv_loader import CsvLoader
from energy_market_etl.etls.etl import Etl


class MarketDataEtl(Etl):
    ETL_METADATA: Dict[str, str] = {
        'market_data': 'energia-elektryczna-rdn',
    }

    def __init__(
            self,
            start_date: dt.datetime,
            end_date: dt.datetime,
            report_type: str,
    ) -> None:
        super().__init__(start_date=start_date, end_date=end_date, report_type=report_type)
        endpoint = MarketDataEtl.ETL_METADATA.get(report_type)
        self.url_provider_factory = UrlProviderFactory(url_type='parametrized', endpoint=endpoint)
        # self.data_access_endpoint = MarketDataEtl.ETL_METADATA.get(report_type)
        # if not self.data_access_endpoint:
        #     raise NotImplementedError('')  # TODO: exception handling + log here

    def extract(self) -> None:
        extract_layer: Extractor = TgeExtractor(
            start_date=self.start_date,
            end_date=self.end_date,
            url_provider_factory=self.url_provider_factory,
        )
        self.__extracted_data = extract_layer.extract()

    def transform(self) -> None:
        transform_layer: Iterable[Transformer] = [
            DateColumnTransformer(date_column_name='Data'), #TODO: argument spec in other place
            StackTransformer(stack_dimension='vertical')
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
