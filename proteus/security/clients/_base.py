from abc import ABC, abstractmethod
from typing import Optional, Dict

from proteus.storage.models.base import DataPath


class ProteusClient(ABC):

    @abstractmethod
    def get_credentials(self):
        pass

    @abstractmethod
    def get_access_token(self, scope: Optional[str] = None) -> str:
        pass

    @abstractmethod
    def connect_storage(self, path: DataPath, set_env: bool = False) -> Optional[Dict]:
        pass

    @abstractmethod
    def connect_account(self):
        pass
