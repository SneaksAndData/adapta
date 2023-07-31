## Astra DB Client

Create a table in Astra and insert some rows:
```cassandraql
create table tmp.test_entity(
    col_a text PRIMARY KEY,
    col_b text,
    col_c text
);

insert into tmp.test_entity (col_a, col_b, col_c) VALUES ('something1', 'else', 'entirely');
insert into tmp.test_entity (col_a, col_b, col_c) VALUES ('something2', 'magic', 'tomorrow');
insert into tmp.test_entity (col_a, col_b, col_c) VALUES ('something3', 'ordinal', 'today');
```

Instantiate a new client, map dataclass (model) to Cassandra model and query it:

```python
from adapta.storage.distributed_object_store.datastax_astra.astra_client import AstraClient

from dataclasses import dataclass, field

@dataclass
class TestEntity:
    col_a: str = field(metadata={
        "is_primary_key": True,
        "is_partition_key": True
    })
    col_b: str
    col_c: str


with AstraClient(
        client_name='test', 
        keyspace='tmp', 
        secure_connect_bundle_bytes='base64string', 
        client_id = 'Astra Token client_id', 
        client_secret = 'Astra Token client_secret'
) as ac:
  single_entity = ac.get_entity('test_entity')
  print(single_entity)
  # {'col_a': 'something', 'col_b': 'else'}

  multiple_entities = ac.get_entities_raw("select * from tmp.test_entity where col_a = 'something3'")
  print(multiple_entities)
  #         col_a     col_b
  # 0  something  ordinal

  print(ac.filter_entities(TestEntity, key_column_filter_values=[{"col_a": 'something1'}]))
  #     col_a col_b     col_c
  # 0  something1  else  entirely

  print(ac.filter_entities(TestEntity, key_column_filter_values=[{"col_a": 'something1'}], select_columns=['col_c']))
  #       col_c
  # 0  entirely
```
