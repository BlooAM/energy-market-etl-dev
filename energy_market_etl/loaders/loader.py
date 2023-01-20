from abc import ABC, abstractmethod

import pandas as pd


class Loader(ABC):
    @abstractmethod
    def load(self, transformed_data: pd.DataFrame):
        raise NotImplementedError('Loader hast to imlpement `load` method')
