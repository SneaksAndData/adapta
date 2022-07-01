# Boxer API Connector

Conenctor for working with [Boxer](https://github.com/SneaksAndData/boxer) AuthZ/AuthN API. 

## Usage

Two environment variables must be set before you can use this connector:

```shell
export BOXER_CONSUMER_ID="my_app_consumer"
export BOXER_PRIVATE_KEY="MIIEpAIBAA..."
```

### Retrieving User Claims:

```python
from proteus.connectors.boxer._connector import BoxerConnector
conn = BoxerConnector(base_url="https://boxer.test.sneaksanddata.com")
claims = conn.get_claims_by_user("user@domain.tld")
for claim in claims:
    print(claim.to_dict())
```

### Retrieving Claims by Type:

```python
from proteus.connectors.boxer._connector import BoxerConnector
conn = BoxerConnector(base_url="https://boxer.test.sneaksanddata.com")
claims = conn.get_claims_by_type("App1.AccessPolicy")
```

### Retrieving Group Claims:

```python
from proteus.connectors.boxer._connector import BoxerConnector
conn = BoxerConnector(base_url="https://boxer.test.sneaksanddata.com")
claims = conn.get_claims_by_group("ad_group_name")
```

### Setting a user claim:
```python
from proteus.connectors.boxer._connector import BoxerConnector
from proteus.connectors.boxer._models import BoxerClaim
claim = BoxerClaim(issuer="App1", claim_type="App1.CanManageConsumers", claim_value="true")
conn = BoxerConnector(base_url="https://boxer.test.sneaksanddata.com")
conn.push_user_claim(claim, "app1admin")
```

### Setting a group claim:
```python
from proteus.connectors.boxer._connector import BoxerConnector
from proteus.connectors.boxer._models import BoxerClaim
claim = BoxerClaim(issuer="App1", claim_type="App1.CanManageConsumers", claim_value="true")
conn = BoxerConnector(base_url="https://boxer.test.sneaksanddata.com")
conn.push_group_claim(claim, "ad_group_name")
```

### Creating a new Auth Consumer (obtaining private key):
```python
from proteus.connectors.boxer._connector import BoxerConnector
conn = BoxerConnector(base_url="https://boxer.test.sneaksanddata.com")
priv_key = conn.create_consumer("app_consumer")
```

### Getting Consumer's public key:
```python
from proteus.connectors.boxer._connector import BoxerConnector
conn = BoxerConnector(base_url="https://boxer.test.sneaksanddata.com")
pub_key = conn.get_consumer_public_key("app_consumer")
```
