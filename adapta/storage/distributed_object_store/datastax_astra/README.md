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

## EXPERIMENTAL - Prototype for generic filtering API.

You can also generate filter expressions for Astra using the new filtering API. Note that this API will be abstracted from the engine in future releases and could also be used with PyArrow expressions. 
Right now only Astra is supported. Example usage:

1. Create a table
```cassandraql
create table tmp.test_entity_new(
    col_a text,
    col_b text,
    col_c int,
    col_d list<int>,
    PRIMARY KEY ( col_a, col_b )
);

insert into tmp.test_entity_new (col_a, col_b, col_c, col_d) VALUES ('something1', 'else', 123, [1, 2]);
insert into tmp.test_entity_new (col_a, col_b, col_c, col_d) VALUES ('something1', 'different', 456, [1, 2, 3]);
insert into tmp.test_entity_new (col_a, col_b, col_c, col_d) VALUES ('something2', 'special', 0, [0, 32, 333]);
```
 2. Create field expressions and apply them
```python
from adapta.storage.distributed_object_store.datastax_astra import AstraField
from adapta.storage.distributed_object_store.datastax_astra.astra_client import AstraClient
from adapta.schema_management.schema_entity import PythonSchemaEntity

from dataclasses import dataclass, field
from typing import List

@dataclass
class TestEntityNew:
    col_a: str = field(metadata={
        "is_primary_key": True,
        "is_partition_key": True
    })
    col_b: str = field(metadata={
        "is_primary_key": True,
        "is_partition_key": False
    })
    col_c: int
    col_d: List[int]

SCHEMA: TestEntityNew = PythonSchemaEntity(TestEntityNew)
simple_filter = (AstraField(SCHEMA.col_a) == "something1")
combined_filter = (AstraField(SCHEMA.col_a) == "something1") & (AstraField(SCHEMA.col_b) == "else")
combined_filter_with_collection = (AstraField(SCHEMA.col_a) == "something1") & (AstraField(SCHEMA.col_b).isin(['else', 'nonexistent']))

with AstraClient(
        client_name='test',
        keyspace='tmp',
        secure_connect_bundle_bytes="base64 bundle string",
        client_id='client id',
        client_secret='client secret'
) as ac:
    print(ac.filter_entities(TestEntityNew, [simple_filter.expression]))
    
    # simple filter field == value    
    #         col_a      col_b  col_c      col_d
    # 0  something1  different    456  [1, 2, 3]
    # 1  something1       else    123     [1, 2]    
    
    print(ac.filter_entities(TestEntityNew, combined_filter.expression))

    #         col_a col_b  col_c   col_d
    # 0  something1  else    123  [1, 2]

    print(ac.filter_entities(TestEntityNew, combined_filter_with_collection.expression))

    #         col_a col_b  col_c   col_d
    # 0  something1  else    123  [1, 2]
```