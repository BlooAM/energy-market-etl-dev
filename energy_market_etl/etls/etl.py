from abc import ABC, abstractmethod


class Etl(ABC):
    @abstractmethod
    def extract(self) -> None:
        raise NotImplementedError('`extract` method not implemented in ETL object')

    @abstractmethod
    def transform(self) -> None:
        raise NotImplementedError('`transform` method not implemented in ETL object')

    @abstractmethod
    def load(self) -> None:
        raise NotImplementedError('`load` method not implemented in ETL object')