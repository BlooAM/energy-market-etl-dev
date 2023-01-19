import collections
import datetime as dt
from typing import Iterable, Union

from energy_market_etl.extractors.extractor import Extractor
from energy_market_etl.transformers.transformer import Transformer
from energy_market_etl.loaders.loader import Loader
from energy_market_etl.extractors.tge.tge_extractor import TgeExtractor
from energy_market_etl.transformers.tge.tge_transformer import TgeTransformer
from energy_market_etl.loaders.tge.tge_loader import TgeLoader
from energy_market_etl.etls.etl import Etl


class TgeEtl(Etl):
    def __init__(
            self,
            start_date: dt.datetime,
            end_date: dt.datetime,
            data_source: str,
    ) -> None:
        self.start_date = start_date
        self.end_date = end_date
        self.data_source = data_source

    def extract(self) -> None:
        extract_layer: Union[Iterable[Extractor], Extractor] = self.__construct_layer()
        if isinstance(extract_layer, collections.abc.Iterable):
            for extractor in extract_layer:
                extractor.extract()
            else:
                extract_layer.extract()

    def transform(self) -> None:
        transform_layer: Union[Iterable[Transformer], Transformer] = self.__construct_layer()
        if isinstance(transform_layer, collections.abc.Iterable):
            for transformer in transform_layer:
                transformer.transform()
            else:
                transform_layer.transform()

    def load(self) -> None:
        load_layer: Union[Iterable[Loader], Loader] = self.__construct_layer()
        if isinstance(load_layer, collections.abc.Iterable):
            for loader in load_layer:
                loader.load()
            else:
                load_layer.load()

    def __construct_layer(self):
        pass
