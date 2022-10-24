from unittest.mock import patch, MagicMock

import pytest

from proteus.security.clients import HashicorpVaultClient
from proteus.security.clients._hashicorp_vault_kubernetes_client import HashicorpVaultKubernetesClient
from proteus.storage.secrets.hashicorp_vault_secret_storage_client import HashicorpSecretStorageClient


@pytest.mark.skip("Uses desktop browser to login, should be only run locally")
def test_oidc_credentials():
    client = HashicorpVaultClient(HashicorpVaultClient.TEST_VAULT_ADDRESS)
    credentials = client.get_credentials()
    assert credentials is not None


@pytest.mark.skip("Uses desktop browser to login, should be only run locally")
def test_oidc_auth():
    client = HashicorpSecretStorageClient(base_client=HashicorpVaultClient(HashicorpVaultClient.TEST_VAULT_ADDRESS))
    secret = client.read_secret("secret", "test/secret/with/path")
    assert secret["key"] == "value"


def test_read_secret_with_mock():
    with patch("hvac.Client", MagicMock(return_value=generate_hashicorp_vault_mock())), \
            patch("webbrowser.open"), \
            patch("proteus.security.clients._hashicorp_vault_client._get_vault_credentials"):
        client = HashicorpSecretStorageClient(base_client=HashicorpVaultClient(HashicorpVaultClient.TEST_VAULT_ADDRESS))
        secret = client.read_secret("secret", "test/secret/with/path")
    assert secret["key"] == "value"


def test_create_secret_with_mock():
    client_mock = generate_hashicorp_vault_mock()

    with patch("hvac.Client", MagicMock(return_value=client_mock)), \
            patch("webbrowser.open"), \
            patch("proteus.security.clients._hashicorp_vault_client._get_vault_credentials"):
        client = HashicorpSecretStorageClient(base_client=HashicorpVaultClient(HashicorpVaultClient.TEST_VAULT_ADDRESS))
        client.create_secret("secret", "path/to/secret", {"key": "value"})

    client_mock.secrets.kv.v2.create_or_update_secret.assert_called_once_with(
        path="path/to/secret",
        secret={'key': 'value'}
    )
    client_mock.secrets.kv.v2.configure.assert_called_once_with(max_versions=20, mount_point="secret")


def test_string_secret():
    client_mock = generate_hashicorp_vault_mock()

    with patch("hvac.Client", MagicMock(return_value=client_mock)), \
            patch("webbrowser.open"), \
            patch("proteus.security.clients._hashicorp_vault_client._get_vault_credentials"):
        client = HashicorpSecretStorageClient(base_client=HashicorpVaultClient(HashicorpVaultClient.TEST_VAULT_ADDRESS))

        with pytest.raises(ValueError) as e:
            client.create_secret("secret", "path/to/secret", '{"key": "value"}')

    assert "Only Dict secret type supported in HashicorpSecretStorageClient but was: <class 'str'>" in str(e.value)
    client_mock.secrets.kv.v2.create_or_update_secret.assert_not_called()
    client_mock.secrets.kv.v2.configure.asssert_not_called()


def test_connect_storage():
    client = HashicorpVaultClient(HashicorpVaultClient.TEST_VAULT_ADDRESS)
    with pytest.raises(NotImplementedError):
        client.connect_storage(MagicMock())


def test_connect_account():
    client = HashicorpVaultClient(HashicorpVaultClient.TEST_VAULT_ADDRESS)
    with pytest.raises(NotImplementedError):
        client.connect_account()


def test_get_pyarrow_filesystem():
    client = HashicorpVaultClient(HashicorpVaultClient.TEST_VAULT_ADDRESS)
    with pytest.raises(NotImplementedError):
        client.get_pyarrow_filesystem(MagicMock())


def generate_hashicorp_vault_mock():
    client_mock = MagicMock()
    client_mock.auth.oidc.oidc_authorization_url_request.return_value = {
        "data": {"auth_url": "https://example.com?nonce=1&state=2"}
    }
    client_mock.secrets.kv.v2.read_secret_version.return_value = {
        "data": {
            "data": {
                "key": "value"
            }
        }
    }
    client_mock.secrets.kv.v2.create_or_update_secret = MagicMock()
    client_mock.secrets.kv.v2.configure = MagicMock()
    return client_mock


# @pytest.mark.skip("Uses desktop browser to login, should be only run locally")
def test_kubernetes_auth():
    client = HashicorpVaultKubernetesClient(HashicorpVaultKubernetesClient.TEST_VAULT_ADDRESS, 'esd-spark-dev')
    assert client.get_credentials() is None


def test_list_secrets():
    client = HashicorpVaultKubernetesClient(HashicorpVaultKubernetesClient.TEST_VAULT_ADDRESS, 'esd-spark-dev')
    client.get_credentials()
    secret_client = HashicorpSecretStorageClient(base_client=client, role="application")
    secrets = list(secret_client.list_secrets("secret", "test"))
    assert secrets == ['test/secret/with/other_path', 'test/secret/with/path']
