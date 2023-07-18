## Astra DB Client

Create a table in Astra and insert some rows:
```cassandraql
create table tmp.test(
    col_a text PRIMARY KEY,
    col_b text
);

insert into tmp.test (col_a, col_b) VALUES ('something1', 'else');
insert into tmp.test (col_a, col_b) VALUES ('something2', 'magic');
insert into tmp.test (col_a, col_b) VALUES ('something3', 'ordinal');
```

Instantiate a new client, map dataclass (model) to Cassandra model and query it:

```python
from adapta.storage.distributed_object_store.datastax_astra.astra_client import AstraClient

from cassandra.cqlengine.models import Model

from dataclasses import dataclass

import pandas

@dataclass
class TestEntity:
    colA: str
    colB: str


with AstraClient(
        client_name='test', 
        keyspace='tmp', 
        secure_connect_bundle_bytes='base64string', 
        client_id = 'Astra Token client_id', 
        client_secret = 'Astra Token client_secret'
) as ac:
  single_entity = ac.get_entity('test')
  print(single_entity)
  # {'col_a': 'something', 'col_b': 'else'}

  multiple_entities = ac.get_entities_raw("select * from tmp.test where col_a = 'something3'")
  print(multiple_entities)
  #         col_a     col_b
  # 0  something  ordinal

  model_class: Model = AstraClient.model_dataclass('test', TestEntity, ['col_a'])
  print(pandas.DataFrame([dict(v.items()) for v in list(model_class.filter(col_a='something1'))]))
  #         col_a col_b
  # 0  something1  else
```
