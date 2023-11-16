# Authentication Clients for various platforms

Currently supported:
- AWS
- HashiCorp Vault
- Azure
- Local (for unit tests)

## Azure Client
There are few important notes on the usage of this client for retrieving storage account credentials via `connect_storage` method. This method is also used by `delta_lake` loaders, thus it is important to understand how to configure your environment and account. The client supports several methods for obtaining credentials, which are listed below in order of preference:

1. Azure Token: If the `PROTEUS__USE_AZURE_CREDENTIAL` environment variable is set, the method returns a token retrieved from Azure AD with the correct scope, and account name.

For the issued tokens to work, this method requires user to have **exactly** `Storage Blob Data Reader` IAM role on the **storage account** or **container**. Please note that even being account owner does not work.

2. Azure Credentials: If environment variables are not found, the method checks for Azure service principal credentials in the environment variables in the form of `AZURE_CLIENT_SECRET` or `PROTEUS__AZURE_CLIENT_SECRET`. If found, it returns the client ID, client secret, tenant ID, and account name.

For the issued tokens to work, this method requires user to have **exactly** `Storage Blob Data Reader` IAM role on the **storage account** or **container**. Please note that even being account owner does not work.

3. Environment Variables (Legacy): The method first checks for mapped environment variables in a format `f'PROTEUS__{STORAGEACCOUNTNAME}_AZURE_STORAGE_ACCOUNT_KEY'`, and if found, returns the account name and key.

Note that this gives your client full write access on the account.

4. Auto-Discovery (Legacy - DO NOT USE): If none of the above options work, the method auto-discovers the storage account using the ARM API and returns the account name and key. If `set_env` is True, it also sets the environment variables for the account name and key.

This method requires user to have `Reader and Data Access` IAM role on the **storage account**.

If none of the above options work, the method raises a ValueError.


## AWS Client

The AWS client allows you to authenticate with the Amazon Web Services cloud using the static credentials (access key id, secret access key and region).

Before using this client, the caller code should invoke the `initialize_session` method.


Currently, only the two types of credentials are supported now:
- `EnvironmentAwsCredentials`: This credential provider loads credentials from the following environment variables:
  - PROTEUS__AWS_SECRET_ACCESS_KEY: Secret access key.
  - PROTEUS__AWS_ACCESS_KEY_ID: Access key id.
  - PROTEUS__AWS_REGION: AWS region.

- `ExplicitAwsCredentials`: This credential provider accepts the credentials as a constructor arguments.
