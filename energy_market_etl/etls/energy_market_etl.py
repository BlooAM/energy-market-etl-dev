from typing import Iterable

from energy_market_etl.extractors.extractor import Extractor
from energy_market_etl.transformers.transformer import Transformer
from energy_market_etl.loaders.loader import Loader
from energy_market_etl.etls.etl import Etl


class EnergyMarketEtl(Etl):
    def __init__(
            self,
            extractors: Iterable[Extractor],
            transformers: Iterable[Transformer],
            loaders: Iterable[Loader]
    ) -> None:
        self.extractors = extractors
        self.transformers = transformers
        self.loaders = loaders

    def extract(self) -> None:
        pass

    def transform(self) -> None:
        pass

    def load(self) -> None:
        pass
