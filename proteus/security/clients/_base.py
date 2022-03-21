from abc import ABC, abstractmethod
from typing import Optional


class ProteusClient(ABC):

    @abstractmethod
    def get_credentials(self):
        pass

    @abstractmethod
    def get_access_token(self, scope: Optional[str] = None) -> str:
        pass

    @abstractmethod
    def connect_storage(self, account_id: Optional[str] = None):
        pass

    @abstractmethod
    def connect_account(self):
        pass
