from abc import ABC, abstractmethod


class EtlExecutor(ABC):
    @abstractmethod
    def execute(self):
        raise NotImplementedError('Executor has to implement `execute` method')
