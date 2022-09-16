from proteus.security.clients import HashicorpVaultClient


def test_get_credentials():
    client = HashicorpVaultClient(HashicorpVaultClient.TEST_VAULT_ADDRESS)
    credentials = client.get_credentials()
    assert credentials is not None
