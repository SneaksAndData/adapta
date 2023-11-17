# Database Clients

Supported clients:

- Generic ODBC
- Azure SQL extension of an ODBC client

Before using any ODBC client, ensure you have the following libraries installed: `ca-certificates gnupg gnupg2 g++ unixodbc-dev`.

## Azure SQL

Instantiate a new client, query, write data out and scale database:

```python
import pandas

from adapta.storage.database.azure_sql import AzureSqlClient
from adapta.logs import SemanticLogger
from adapta.logs.models import LogLevel

c_logger = SemanticLogger().add_log_source(
    log_source_name='azsql',
    min_log_level=LogLevel.INFO,
    is_default=True,
    log_handlers=[]  # don't forget to provide log handlers if you need to log outside stdout
)

with AzureSqlClient(
        logger=c_logger,
        host_name='my-sql-host',
        user_name='my-sql-user',
        password='my-sql-password',
        database='my-database'
) as azsql:
    # read from dbo.big_data into an iterable of pandas dataframes
    some_data = azsql.query('select * from dbo.big_data', chunksize=1000)

    for chunk in some_data:
        print(chunk)

    # write data to dbo.small_data
    data_to_write = pandas.DataFrame(data={
        'id': ["1", "2", "3"],
        'name': ["Exostrike", "BIOM", "Collin"]
    })

    azsql.materialize(data_to_write, 'dbo', 'small_data', True)

    # scale Azure SQL instance
    result = azsql.scale_instance(target_objective='HS_Gen4_1', max_wait_time=300)
```

## Trino (www.trino.io)

Note that each context invocation with OAuth2 will open a browser tab, but all queries performed inside the `with` block will reuse the fetched token.

```python
import os
import pandas
from adapta.storage.database.trino_sql import TrinoClient

# use Basic Auth
os.environ['PROTEUS__TRINO_USERNAME'] = 'foo'
os.environ['PROTEUS__TRINO_PASSWORD'] = 'bar'
tc_basic_auth = TrinoClient(host="trino.production.sneaksanddata.com", catalog="trinodatalake")

# use OAuth2 (interactive browser)
os.environ['PROTEUS__TRINO_OAUTH2_USERNAME'] = 'ME@ecco.com'
tc_oauth2 = TrinoClient(host="trino.production.sneaksanddata.com", catalog="trinodatalake")

# query a table using Basic auth and print results
with tc_basic_auth as tc:
    for frame in tc.query('select * from bronze.tcurr limit 1'):
        print(frame)

# query a table using OAuth2 aggregate results into a single dataframe
with tc_oauth2 as tc:
    result = pandas.concat(tc.query('select * from bronze.tcurr limit 1'))
    print(result)
```

## Snowflake
Initialize a Snowflake client and run queries. Each context invocation will open a browser tab, but all queries performed inside the `with` block will reuse the fetched token.
```python
from adapta.storage.database.snowflake_sql import SnowflakeClient

snowflake_client = SnowflakeClient(user="email@email.com", account="ACCOUNT", warehouse="WAREHOUSE")

query = "SELECT * FROM datalake.tt limit 10"

with snowflake_client as sc:
    result = sc.query(query)
    print(result)
```