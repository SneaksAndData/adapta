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

## EXPERIMENTAL - Generic Filtering API.
Generate filter expressions and compile them for Astra or for PyArrow expressions. Example usage:
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
from adapta.storage.models.filter_expression import (
    FilterField,
    ArrowFilterExpression,
    AstraFilterExpression,
    compile_expression
)
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
# Create generic filters
simple_filter = FilterField[str](SCHEMA.col_a) == "something1"
combined_filter = (FilterField[str](SCHEMA.col_a) == "something1") & (FilterField[str](SCHEMA.col_b) == "else")
combined_filter_with_collection = (FilterField[str](SCHEMA.col_a) == "something1") & (FilterField[str](SCHEMA.col_b).isin(['else', 'nonexistent']))
complex_filter = (FilterField[str](SCHEMA.col_a) == "something1") | (FilterField[str](SCHEMA.col_b) == "else") & (FilterField[int](SCHEMA.col_c) == 123)

# Compile the filters for Astra
simple_expression_astra = compile_expression(simple_filter, AstraFilterExpression)
combined_expression_astra = compile_expression(combined_filter, AstraFilterExpression)
combined_expression_with_collection_astra = compile_expression(combined_filter_with_collection, AstraFilterExpression)
complex_expression_astra = compile_expression(complex_filter, AstraFilterExpression)

# Compile filters for PyArrow
simple_expression_astra = compile_expression(simple_filter, ArrowFilterExpression)
combined_expression_astra = compile_expression(combined_filter, ArrowFilterExpression)
combined_expression_with_collection_astra = compile_expression(combined_filter_with_collection, ArrowFilterExpression)
complex_expression_astra = compile_expression(complex_filter, ArrowFilterExpression)

# Apply the filters for Astra
with AstraClient(
        client_name='test',
        keyspace='tmp',
        secure_connect_bundle_bytes="base64 bundle string",
        client_id='client id',
        client_secret='client secret'
) as ac:
    print(ac.filter_entities(TestEntityNew, simple_expression_astra))
    
    # simple filter field == value    
    #         col_a      col_b  col_c      col_d
    # 0  something1  different    456  [1, 2, 3]
    # 1  something1       else    123     [1, 2]    
    
    print(ac.filter_entities(TestEntityNew, combined_expression_astra))

    #         col_a col_b  col_c   col_d
    # 0  something1  else    123  [1, 2]

    print(ac.filter_entities(TestEntityNew, combined_expression_with_collection_astra))

    #         col_a col_b  col_c   col_d
    # 0  something1  else    123  [1, 2]

   print(ac.filter_entities(TestEntityNew, complex_expression_astra))
    #         col_a col_b  col_c   col_d
    # 0  something1  else    123  [1, 2]

```