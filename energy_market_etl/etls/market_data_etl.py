import collections
import datetime as dt
from typing import Iterable, Union

from energy_market_etl.extractors.extractor import Extractor
from energy_market_etl.transformers.transformer import Transformer
from energy_market_etl.loaders.loader import Loader
from energy_market_etl.extractors.tge.tge_extractor import TgeExtractor
from energy_market_etl.transformers.date_column.date_column_transformer import DateColumnTransformer
from energy_market_etl.transformers.vertical_stack.vertical_stack_transformer import VerticalStackTransformer
from energy_market_etl.loaders.csv.csv_loader import CsvLoader
from energy_market_etl.etls.etl import Etl


class MarketDataEtl(Etl):
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

    def extract(self) -> None:
        extract_layer: Union[Iterable[Extractor], Extractor] = TgeExtractor(

        )
        if isinstance(extract_layer, collections.abc.Iterable):
            for extractor in extract_layer:
                extractor.extract()
            else:
                extract_layer.extract()

    def transform(self) -> None:
        transform_layer: Iterable[Transformer] = [
            DateColumnTransformer(date_column_name='Data'), #TODO: argument spec in other place
            VerticalStackTransformer()
        ]
        for transformer in transform_layer:
            transformer.transform()

    def load(self) -> None:
        load_layer: Loader = CsvLoader(
            file_name=self.report_name
        )
        load_layer.load()
