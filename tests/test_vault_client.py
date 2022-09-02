from proteus.security.clients import VaultClient


def test_get_credentials():
    client = VaultClient()
    credentials = client.get_credentials()
    assert credentials is not None
