# Secret Storage Integrations

Supported API:
- read a secret from an Azure Key Vault
- save a plaintext secret with optional base64 encoding in an Azure Key Vault

## Example usage

### Azure 
```python
from adapta.security.clients import AzureClient
from adapta.storage.secrets.azure_secret_client import AzureSecretStorageClient

azure_client = AzureClient(subscription_id='test')
azure_secrets = AzureSecretStorageClient(base_client=azure_client)

my_secret = azure_secrets.read_secret('my-keyvault', 'my-secret')

print(my_secret)
```

### Hashicorp vault
```python
from adapta.security.clients import HashicorpVaultOidcClient, HashicorpVaultTokenClient
```
Create a HashicorpVaultTokenClient Instance
To read secrets from HashiCorpVault, you need to create a HashicorpVaultTokenClient instance. This
instance requires the address of your HashiCorpVault instance and an access token fetched from HashicorpVaultOidcClient
to authenticate the client. You must run this code on a machine that has access to a web browser:

```python
oidc_client = HashicorpVaultOidcClient(TEST_VAULT_ADDRESS)
access_token = oidc_client.get_access_token()
token_client = HashicorpVaultTokenClient(TEST_VAULT_ADDRESS, access_token)
client = HashicorpSecretStorageClient(base_client=token_client)
```

Read Secrets from HashiCorpVault
Once you have created a HashicorpVaultTokenClient instance,
you can read secrets from HashiCorpVault using the read_secret() method. This method takes two arguments:
the name of the secret engine and the path to the secret:

```python
secret = client.read_secret("secret", "test/secret/with/path")
```
The above code reads a secret from the "secret" engine located at "test/secret/with/path".

Access Secret Values
Once you have read a secret, you can access its values using the key-value pairs returned by the read_secret() method:

```python
print(secret["key"])
```
The above code prints the value of the "key" field in the secret.
