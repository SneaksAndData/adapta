import os
from typing import Optional, List, Dict, Tuple

from azure.mgmt.storage.v2021_08_01.models import StorageAccountKey, StorageAccount
from azure.mgmt.storage import StorageManagementClient
from azure.identity import DefaultAzureCredential

from proteus.security.clients._base import ProteusClient
from proteus.storage.models.azure import AdlsGen2Path
from proteus.storage.models.base import DataPath


class AzureClient(ProteusClient):
    """
     Azure Credentials provider for various Azure resources.
    """
    def __init__(self, *, subscription_id: str):
        self.subscription_id = subscription_id

    def get_access_token(self, scope: Optional[str] = None) -> str:
        return DefaultAzureCredential(exclude_shared_token_cache_credential=True) \
            .get_token(scope or "https://management.core.windows.net/.default").token

    def connect_account(self):
        """
         Not used in Azure.
        :return:
        """
        pass

    def connect_storage(self, path: DataPath, set_env: bool = False) -> Optional[Dict]:
        def get_resource_group(account: StorageAccount) -> str:
            return account.id.split('/')[account.id.split('/').index('resourceGroups') + 1]

        assert isinstance(path, AdlsGen2Path), 'Azure Client only works with proteus.storage.models.azure.AdlsGen2Path'

        adls_path: AdlsGen2Path = path

        if not os.environ.get('AZURE_STORAGE_ACCOUNT_NAME', None):
            cred = DefaultAzureCredential(exclude_shared_token_cache_credential=True)
            storage_client = StorageManagementClient(cred, self.subscription_id)

            accounts: List[Tuple[str, str]] = list(map(lambda result: (get_resource_group(result), result.name), storage_client.storage_accounts.list()))

            for rg, account in accounts:
                if adls_path.account == account:
                    keys: List[StorageAccountKey] = storage_client.storage_accounts.list_keys(
                        resource_group_name=rg,
                        account_name=account).keys

                    if set_env:
                        os.environ.setdefault('AZURE_STORAGE_ACCOUNT_NAME', account)
                        os.environ.setdefault('AZURE_STORAGE_ACCOUNT_KEY', keys[0].value)

                        return None

                    return {
                        'AZURE_STORAGE_ACCOUNT_NAME': account,
                        'AZURE_STORAGE_ACCOUNT_KEY': keys[0].value
                    }

            raise ValueError(f"Can't locate an account {path.account}")

        return None

    def get_credentials(self) -> DefaultAzureCredential:
        return DefaultAzureCredential(exclude_shared_token_cache_credential=True)
