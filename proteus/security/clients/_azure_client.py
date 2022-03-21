import os
from typing import Optional, List

from azure.mgmt.storage.v2021_08_01.models import StorageAccountKey

from proteus.security.clients._base import ProteusClient
from azure.identity import DefaultAzureCredential
from azure.mgmt.storage import StorageManagementClient


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

    def connect_storage(self, account_id: Optional[str] = None):
        assert '/' in account_id, 'Account id for Azure must follow the following format: <resource_group>/<account>'

        rg, account = account_id.split('/')[:2]

        if not os.environ.get('AZURE_STORAGE_ACCOUNT_NAME', None):
            cred = DefaultAzureCredential(exclude_shared_token_cache_credential=True)
            storage_client = StorageManagementClient(cred, self.subscription_id)

            keys: List[StorageAccountKey] = storage_client.storage_accounts.list_keys(resource_group_name=rg,
                                                                                      account_name=account).keys
            os.environ.setdefault('AZURE_STORAGE_ACCOUNT_NAME', account)
            os.environ.setdefault('AZURE_STORAGE_ACCOUNT_KEY', keys[0].value)

    def get_credentials(self) -> DefaultAzureCredential:
        return DefaultAzureCredential(exclude_shared_token_cache_credential=True)
