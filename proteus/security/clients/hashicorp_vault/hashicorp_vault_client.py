"""
 Hashicorp Vault implementation of Proteus Client.
"""
from abc import abstractmethod
from typing import Optional, Dict

from pyarrow.fs import FileSystem

from proteus.security.clients._base import ProteusClient
from proteus.storage.models.base import DataPath


class HashicorpVaultClient(ProteusClient):
    """
     Hashicorp vault Credentials provider.
    """

    @staticmethod
    def from_base_client(client: ProteusClient) -> Optional['HashicorpVaultClient']:
        """
         Safe casts ProteusClient to HashicorpVaultClient if type checks out.

        :param client: ProteusClient
        :return: AbstractHashicorpVaultClient or None if type does not check out
        """
        if isinstance(client, HashicorpVaultClient):
            return client

        return None

    def __init__(self, vault_address: str):
        """
        Common initialization logic for hashicorp vault clients
        :param vault_address: Address of hashicorp vault instance
        """
        self._vault_address = vault_address

    @property
    def vault_address(self):
        """Returns address of Hashicorp Vault server"""
        return self._vault_address

    def connect_storage(self, path: DataPath, set_env: bool = False) -> Optional[Dict]:
        """
         Not supported  in AbstractHashicorpVaultClient
        :return:
        """
        raise ValueError("Not supported  in HashicorpVaultClient")

    def connect_account(self):
        """
         Not supported  in AbstractHashicorpVaultClient
        :return:
        """
        raise ValueError("Not supported  in HashicorpVaultClient")

    def get_pyarrow_filesystem(self, path: DataPath) -> FileSystem:
        """
         Not supported  in AbstractHashicorpVaultClient
        :return:
        """
        raise ValueError("Not supported  in HashicorpVaultClient")

    @abstractmethod
    def get_credentials(self):
        pass

    @abstractmethod
    def get_access_token(self, scope: Optional[str] = None) -> str:
        pass
