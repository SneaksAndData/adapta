## Astra DB Client

Create a table in Astra and insert some rows:
```cassandraql
create table ks.test(
    colA text PRIMARY KEY,
    colB text
);

insert into ks.test (colA, colB) VALUES ('something', 'else');
insert into ks.test (colA, colB) VALUES ('something', 'magic');
insert into ks.test (colA, colB) VALUES ('something', 'ordinal');
```

Instantiate a new client, map dataclass (model) to Cassandra model and query it:

```python
from adapta.storage.distributed_object_store.datastax_astra.astra_client import AstraClient

from cassandra.cqlengine.models import Model

from dataclasses import dataclass

@dataclass
class TestEntity:
    colA: str
    colB: str


with AstraClient(
        client_name='test', 
        keyspace='ks', 
        secure_connect_bundle_bytes='base64string', 
        client_id = 'Astra Token client_id', 
        client_secret = 'Astra Token client_secret'
) as ac:
  single_entity = ac.get_entity('my_table')
  print(single_entity)
  # {'colA': 'something', 'colB': 'else'}

  multiple_entities = ac.get_entities_raw("select * from ks.test where colA = 'something'")
  print(multiple_entities)
  # {'colA': 'something', 'colB': 'else'}
  # {'colA': 'something', 'colB': 'magic'}
  # {'colA': 'something', 'colB': 'ordinal'}

  model_class: Model = AstraClient.model_dataclass(TestEntity, ['colA'])
  print(model_class.filter(colB='magic'))
  # [{'colA': 'something', 'colB': 'magic'}]
```
