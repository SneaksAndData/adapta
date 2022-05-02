"""
 Azure Cloud implementation of Proteus Client.
"""
import os
from typing import Optional, List, Dict, Tuple

from azure.mgmt.storage.v2021_08_01.models import StorageAccountKey, StorageAccount
from azure.mgmt.storage import StorageManagementClient
from azure.identity import DefaultAzureCredential

from proteus.security.clients._base import ProteusClient
from proteus.storage.models.azure import AdlsGen2Path
from proteus.storage.models.base import DataPath


def _get_azure_credentials() -> DefaultAzureCredential:
    """
      Returns credentials for Azure Cloud going through credential provider chain.

    :return: DefaultAzureCredential
    """
    return DefaultAzureCredential(
        exclude_shared_token_cache_credential=True,
        exclude_visual_studio_code_credential=True,
        exclude_powershell_credential=True
    )


class AzureClient(ProteusClient):
    """
     Azure Credentials provider for various Azure resources.
    """

    def __init__(self, *, subscription_id: str):
        self.subscription_id = subscription_id

    @classmethod
    def from_base_client(cls, client: ProteusClient) -> Optional['AzureClient']:
        if isinstance(client, AzureClient):
            return client

        return None

    def get_access_token(self, scope: Optional[str] = None) -> str:
        return _get_azure_credentials().get_token(scope or "https://management.core.windows.net/.default").token

    def connect_account(self):
        """
         Not used in Azure.
        :return:
        """

    def connect_storage(self, path: DataPath, set_env: bool = False) -> Optional[Dict]:
        def get_resource_group(account: StorageAccount) -> str:
            return account.id.split('/')[account.id.split('/').index('resourceGroups') + 1]

        assert isinstance(path, AdlsGen2Path), 'Azure Client only works with proteus.storage.models.azure.AdlsGen2Path'

        adls_path: AdlsGen2Path = path

        storage_client = StorageManagementClient(_get_azure_credentials(), self.subscription_id)

        accounts: List[Tuple[str, str]] = list(
            map(lambda result: (get_resource_group(result), result.name), storage_client.storage_accounts.list()))

        for rg, account in accounts:  # pylint: disable=C0103
            if adls_path.account == account:
                keys: List[StorageAccountKey] = storage_client.storage_accounts.list_keys(
                    resource_group_name=rg,
                    account_name=account).keys

                if set_env:
                    os.environ.update({'AZURE_STORAGE_ACCOUNT_NAME': account})
                    os.environ.update({'AZURE_STORAGE_ACCOUNT_KEY': keys[0].value})

                return {
                    'AZURE_STORAGE_ACCOUNT_NAME': account,
                    'AZURE_STORAGE_ACCOUNT_KEY': keys[0].value
                }

        raise ValueError(f"Can't locate an account {path.account}")

    def get_credentials(self) -> DefaultAzureCredential:
        return _get_azure_credentials()
