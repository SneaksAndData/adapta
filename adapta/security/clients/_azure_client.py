"""
 Azure Cloud implementation of AuthenticationClient.
"""
#  Copyright (c) 2023-2024. ECCO Sneaks & Data
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

import os

import logging

from typing import Optional, List, Dict, Tuple, Union

from adlfs import AzureBlobFileSystem
from azure.mgmt.storage.v2021_08_01.models import StorageAccountKey, StorageAccount
from azure.mgmt.storage import StorageManagementClient
from azure.identity import DefaultAzureCredential
from pyarrow.fs import PyFileSystem, FSSpecHandler, SubTreeFileSystem, FileSystem

from adapta.security.clients._base import AuthenticationClient
from adapta.storage.models.azure import AdlsGen2Path, WasbPath
from adapta.storage.models.base import DataPath


def _get_azure_credentials() -> DefaultAzureCredential:
    """
      Returns credentials for Azure Cloud going through credential provider chain.

    :return: DefaultAzureCredential
    """
    return DefaultAzureCredential(
        exclude_shared_token_cache_credential=True,
        exclude_visual_studio_code_credential=True,
        exclude_powershell_credential=True,
    )


class AzureClient(AuthenticationClient):
    """
    Azure Credentials provider for various Azure resources.
    """

    def __init__(self, *, subscription_id: Optional[str] = None, default_log_level=logging.ERROR):
        self.subscription_id = subscription_id

        # disable Azure CLI telemetry collection as it is not thread-safe
        os.environ["AZURE_CORE_COLLECT_TELEMETRY"] = "0"

        # disable Azure CLI prompt confirmations
        os.environ["AZURE_CORE_DISABLE_CONFIRM_PROMPT"] = "1"

        logger = logging.getLogger("azure")
        logger.setLevel(default_log_level)

    @classmethod
    def from_base_client(cls, client: AuthenticationClient) -> Optional["AzureClient"]:
        """
         Safe casts AuthenticationClient to AzureClient if type checks out.

        :param client: AuthenticationClient
        :return: AzureClient or None if type does not check out
        """
        if isinstance(client, AzureClient):
            return client

        return None

    def _get_default_token(self):
        return _get_azure_credentials().get_token("https://management.core.windows.net/.default").token

    def get_access_token(self, scope: Optional[str] = None) -> str:
        if scope:
            return _get_azure_credentials().get_token(scope).token
        return self._get_default_token()

    def connect_account(self):
        """
         Not used in Azure.
        :return:
        """

    def connect_storage(self, path: DataPath, set_env: bool = False) -> Optional[Dict]:
        def get_resource_group(account: StorageAccount) -> str:
            return account.id.split("/")[account.id.split("/").index("resourceGroups") + 1]

        assert isinstance(
            path, (AdlsGen2Path, WasbPath)
        ), "Only adapta.storage.models.azure.AdlsGen2Path or with adapta.storage.models.azure.WasbPath are supported"

        adls_path: Union[AdlsGen2Path, WasbPath] = path

        # rely on mapped env vars, if they exist
        if f"PROTEUS__{adls_path.account.upper()}_AZURE_STORAGE_ACCOUNT_KEY" in os.environ:
            return {
                "AZURE_STORAGE_ACCOUNT_NAME": adls_path.account,
                "AZURE_STORAGE_ACCOUNT_KEY": os.getenv(
                    f"PROTEUS__{adls_path.account.upper()}_AZURE_STORAGE_ACCOUNT_KEY"
                ),
            }

        if "AZURE_CLIENT_SECRET" in os.environ or "PROTEUS__AZURE_CLIENT_SECRET" in os.environ:
            return {
                "AZURE_CLIENT_ID": os.getenv("AZURE_CLIENT_ID") or os.getenv("PROTEUS__AZURE_CLIENT_ID"),
                "AZURE_CLIENT_SECRET": os.getenv("AZURE_CLIENT_SECRET") or os.getenv("PROTEUS__AZURE_CLIENT_SECRET"),
                "AZURE_TENANT_ID": os.getenv("AZURE_TENANT_ID") or os.getenv("PROTEUS__AZURE_TENANT_ID"),
                "ACCOUNT_NAME": adls_path.account,
            }

        if "PROTEUS__USE_AZURE_CREDENTIAL" in os.environ:
            return {
                "TOKEN": _get_azure_credentials().get_token("https://storage.azure.com/.default").token,
                "ACCOUNT_NAME": adls_path.account,
            }

        if self.subscription_id:
            # Auto discover through ARM if env vars are not present for the target account
            storage_client = StorageManagementClient(_get_azure_credentials(), self.subscription_id)

            accounts: List[Tuple[str, str]] = list(
                map(
                    lambda result: (get_resource_group(result), result.name),
                    storage_client.storage_accounts.list(),
                )
            )

            for rg, account in accounts:  # pylint: disable=C0103
                if adls_path.account == account:
                    keys: List[StorageAccountKey] = storage_client.storage_accounts.list_keys(
                        resource_group_name=rg, account_name=account
                    ).keys

                    if set_env:
                        os.environ.update({"AZURE_STORAGE_ACCOUNT_NAME": account})
                        os.environ.update({"AZURE_STORAGE_ACCOUNT_KEY": keys[0].value})

                    return {
                        "AZURE_STORAGE_ACCOUNT_NAME": account,
                        "AZURE_STORAGE_ACCOUNT_KEY": keys[0].value,
                    }

        raise ValueError(f"Can't find credentials for an account {adls_path.account}")

    def get_credentials(self) -> DefaultAzureCredential:
        return _get_azure_credentials()

    def get_pyarrow_filesystem(self, path: DataPath, connection_options: Optional[Dict[str, str]] = None) -> FileSystem:
        def select_file_system(options: Optional[Dict[str, str]], account_name: str) -> AzureBlobFileSystem:
            if not options and "PROTEUS__USE_AZURE_CREDENTIAL" in os.environ:
                return AzureBlobFileSystem(account_name=account_name, anon=False)

            if not options and f"PROTEUS__{account_name.upper()}_AZURE_STORAGE_ACCOUNT_KEY" in os.environ:
                return AzureBlobFileSystem(
                    account_name=account_name,
                    account_key=os.getenv(f"PROTEUS__{account_name.upper()}_AZURE_STORAGE_ACCOUNT_KEY"),
                )

            if not options and ("AZURE_CLIENT_SECRET" in os.environ or "PROTEUS__AZURE_CLIENT_SECRET" in os.environ):
                return AzureBlobFileSystem(
                    account_name=account_name,
                    client_id=os.getenv("AZURE_CLIENT_ID") or os.getenv("PROTEUS__AZURE_CLIENT_ID"),
                    client_secret=os.getenv("AZURE_CLIENT_SECRET") or os.getenv("PROTEUS__AZURE_CLIENT_SECRET"),
                    tenant_id=os.getenv("AZURE_TENANT_ID") or os.getenv("PROTEUS__AZURE_TENANT_ID"),
                )

            if options.get("AZURE_STORAGE_ACCOUNT_KEY", None):
                return AzureBlobFileSystem(
                    account_name=account_name,
                    account_key=options["AZURE_STORAGE_ACCOUNT_KEY"],
                )

            if options.get("AZURE_CLIENT_SECRET", None):
                return AzureBlobFileSystem(
                    account_name=account_name,
                    client_id=options["AZURE_CLIENT_ID"],
                    client_secret=options["AZURE_CLIENT_SECRET"],
                    tenant_id=options["AZURE_TENANT_ID"],
                )

            raise ValueError(f"Unsupported connection options have been provided: {connection_options}")

        assert isinstance(
            path, (AdlsGen2Path, WasbPath)
        ), "Only adapta.storage.models.azure.AdlsGen2Path or with adapta.storage.models.azure.WasbPath are supported"

        return SubTreeFileSystem(
            path.to_hdfs_path(), PyFileSystem(FSSpecHandler(select_file_system(connection_options, path.account)))
        )
