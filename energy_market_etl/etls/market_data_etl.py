import datetime as dt
from typing import Dict, Iterable

from energy_market_etl.utils.class_metadata_utils import class_names
from energy_market_etl.utils.url_utils import UrlProviderFactory
from energy_market_etl.extractors.extractor import Extractor
from energy_market_etl.transformers.transformer import Transformer
from energy_market_etl.loaders.loader import Loader
from energy_market_etl.extractors.tge.tge_extractor import TgeExtractor
from energy_market_etl.transformers.date_column.date_column_transformer import DateColumnTransformer
from energy_market_etl.transformers.stack.stack_transformer import StackTransformer
from energy_market_etl.transformers.metadata.metadata_transformer import MetadataTransformer
from energy_market_etl.loaders.csv.csv_loader import CsvLoader
from energy_market_etl.loaders.google_storage.google_cloud_storage_loader import GoogleCloudStorageLoader
from energy_market_etl.etls.etl import Etl, read_config


class MarketDataEtl(Etl):
    ETL_METADATA: Dict[str, str] = {
        'tge_rdn_hourly_data': 'energia-elektryczna-rdn',
        'tge_rdn_index_data': 'energia-elektryczna-rdn',
    }

    def __init__(
            self,
            start_date: dt.datetime,
            end_date: dt.datetime,
            report_type: str,
    ) -> None:
        super().__init__(start_date=start_date, end_date=end_date, report_type=report_type)
        endpoint = MarketDataEtl.ETL_METADATA.get(report_type, '')
        self.__config = read_config()
        self.url_provider_factory = UrlProviderFactory(url_type='parametrized', endpoint=endpoint)
        self.index_data = True if report_type == 'tge_rdn_index_data' else False

    def extract(self) -> None:
        extract_layer: Extractor = TgeExtractor(
            start_date=self.start_date,
            end_date=self.end_date,
            url_provider_factory=self.url_provider_factory,
            index_data=self.index_data
        )
        self.__extracted_data = extract_layer.extract()

    def transform(self) -> None:
        transform_layer: Iterable[Transformer] = [
            DateColumnTransformer(date_column_name='Data'),
            StackTransformer(stack_dimension='vertical'),
            MetadataTransformer(reset_index=True),
        ]
        self.__transformed_data = self.__extracted_data.copy()
        for transformer in transform_layer:
            self.__transformed_data = transformer.transform(self.__transformed_data)

    def load(self) -> None:
        load_layer: Iterable[Loader] = [
            CsvLoader(file_name=self.report_name),
            GoogleCloudStorageLoader(
                file_name=self.report_name,
                config=self.__config.google_storage,
            ),
        ]
        for loader in load_layer:
            loader.load(self.__transformed_data)


__all__ = class_names(MarketDataEtl)
