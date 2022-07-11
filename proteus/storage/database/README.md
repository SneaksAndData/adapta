# Database Clients

Supported clients:

- Generic ODBC
- Azure SQL extension of an ODBC client

Before using any ODBC client, ensure you have the following libraries installed: `ca-certificates gnupg gnupg2 g++ unixodbc-dev`.

## Azure SQL

Instantiate a new client, query, write data out and scale database:

```python
import pandas

from proteus.storage.database.azure_sql import AzureSqlClient
from proteus.logs import ProteusLogger
from proteus.logs.models import LogLevel

proteus_logger = ProteusLogger().add_log_source(
    log_source_name='azsql', 
    min_log_level=LogLevel.INFO, 
    is_default=True, 
    log_handlers=[]  # don't forget to provide log handlers if you need to log outside stdout
)

azsql = AzureSqlClient(
    logger=proteus_logger,
    host_name='my-sql-host',
    user_name='my-sql-user',
    password='my-sql-password',
    database='my-database'
)

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
result = azsql.scale_instance(target_objective='BFG_INSTANCE', timeout_seconds=300)
```

