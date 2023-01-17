from abc import ABC, abstractmethod


class Transformer(ABC):
    @abstractmethod
    def transform(self):
        raise NotImplementedError('Transformer has to implement `transform` method')
