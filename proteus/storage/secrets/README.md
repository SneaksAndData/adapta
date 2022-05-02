# Secret Storage Integrations

Supported API:
- read a secret from an Azure Key Vault
- save a plaintext secret with optional base64 encoding in an Azure Key Vault

## Example usage

```python
from proteus.security.clients import AzureClient
from proteus.storage.secrets.azure_secret_client import AzureSecretStorageClient

azure_client = AzureClient(subscription_id='test')
azure_secrets = AzureSecretStorageClient(base_client=azure_client)

my_secret = azure_secrets.read_secret('my-keyvault', 'my-secret')

print(my_secret)
```
