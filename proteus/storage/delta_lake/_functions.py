import pandas
from deltalake import DeltaTable

from proteus.security.clients import AzureClient
from proteus.storage.models.azure import AdlsGen2Path


def load_from_adls2(azure_client: AzureClient, resource_group: str, path: AdlsGen2Path) -> pandas.Dataframe:
    azure_client.connect_storage(f"{resource_group}/{path.account}")

    return DeltaTable(path.to_delta_rs_path()).to_pandas()
