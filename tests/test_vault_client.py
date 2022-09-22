import pytest

from proteus.security.clients import HashicorpVaultClient
from proteus.storage.secrets.hashicorp_vault_client import HashicorpSecretStorageClient


@pytest.mark.skip("Uses desktop browser to login, should be only run locally")
def test_get_credentials():
    client = HashicorpVaultClient(HashicorpVaultClient.TEST_VAULT_ADDRESS)
    credentials = client.get_credentials()
    assert credentials is not None


#@pytest.mark.skip("Uses desktop browser to login, should be only run locally")
def test_read_secret():
    client = HashicorpSecretStorageClient(base_client=HashicorpVaultClient(HashicorpVaultClient.TEST_VAULT_ADDRESS))
    credentials = client.read_secret("secret", "test/secret/with/path")
    assert credentials is not None
