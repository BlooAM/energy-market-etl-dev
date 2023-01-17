from abc import ABC, abstractmethod


class Extractor(ABC):
    @abstractmethod
    def extract(self):
        raise NotImplementedError('Extractor has to implement `extract` method')
