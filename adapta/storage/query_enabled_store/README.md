## Query-Enabled Store Interface

This API greatly simplifies data reads for applications that require transparent swap of a data backend, for example Delta -> Cassandra, Delta (Azure) -> Delta (AWS) or any other combination. Implementing a QES client allows data scientists and engineers to write applications that can be reconfigured to run on any supported store by simply updating the application's configuration.

QES enables abstraction of *both* storage connection and filtering API, which relies on adapta's `FilterExpression` API.

### Usage

You can create a QES instance by providing a QES-formatted connection string. Two formats are supported:
- `"qes://class=<full class name>;plaintext_credentials=<json object>;settings=<json object>"`
- `"qes://class=<stable store alias>;plaintext_credentials=<json object>;settings=<json object>"`

Some QES implementations are bundled with `adapta`: `ASTRA`, `DELTA`. In case you have implemented your own QES in this repository, you can add it to `BundledQes` enum, or it can be loaded dynamically from client code, if your application is able to resolve the QES class import.

Example of a connection string, for a delta table stored on Azure:
```python
# using dynamic import
conn = "qes://engine=adapta.storage.query_enabled.DeltaQes;plaintext_credentials={\"auth_client_class\":\"adapta.security.clients.AzureClient\"};settings={}"
# using bundled QES
conn_bundled = "qes://engine=DELTA;plaintext_credentials={\"auth_client_class\":\"adapta.security.clients.AzureClient\"};settings={}"
```
Now, initialize a QES object from that connection and read some data:

```python
import os
from adapta.storage.query_enabled_store import QueryEnabledStore
from adapta.storage.models.azure import AdlsGen2Path
from adapta.storage.models.filter_expression import FilterField

# use implicit auth for Azure (for simplicity - check AzureClient documentation for more options)
os.environ["PROTEUS__USE_AZURE_CREDENTIAL"] = "1"
adls_path = AdlsGen2Path.from_hdfs_path('abfss://container@account.dfs.core.windows.net/path/to/table')

conn = "qes://engine=DELTA;plaintext_credentials={\"auth_client_class\":\"adapta.security.clients.AzureClient\"};settings={}"
store = QueryEnabledStore.from_string(conn)
data = store.open(adls_path).filter(FilterField("date_key") == "20230101").select("date_key", "year").read()

print(data)

#
#    date_key  year
# 0  20230101  2023
```
