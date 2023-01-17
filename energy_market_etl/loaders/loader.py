from abc import ABC, abstractmethod


class Loader(ABC):
    @abstractmethod
    def load(self):
        raise NotImplementedError('Loader hast to imlpement `load` method')
