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

## Using the Filtering API.
Generate filter expressions and compile them for Astra or for PyArrow expressions. Example usage:
1. Create a table
```cassandraql
create table tmp.test_entity_new(
    col_a text,
    col_b text,
    col_c int,
    PRIMARY KEY ( col_a, col_b )
);

insert into tmp.test_entity_new (col_a, col_b, col_c) VALUES ('something1', 'else', 123);
insert into tmp.test_entity_new (col_a, col_b, col_c) VALUES ('something1', 'different', 456);
insert into tmp.test_entity_new (col_a, col_b, col_c) VALUES ('something2', 'special', 0);
```

2. Create field expressions and apply them
```python
from adapta.storage.models.filter_expression import FilterField
from adapta.storage.distributed_object_store.datastax_astra.astra_client import AstraClient
from adapta.schema_management.schema_entity import PythonSchemaEntity

from dataclasses import dataclass, field

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


SCHEMA: TestEntityNew = PythonSchemaEntity(TestEntityNew)
# Create generic filters
simple_filter = FilterField(SCHEMA.col_a) == "something1"
combined_filter = (FilterField(SCHEMA.col_a) == "something1") & (FilterField(SCHEMA.col_b) == "else")
combined_filter_with_collection = (FilterField(SCHEMA.col_a) == "something1") & (FilterField(SCHEMA.col_b).isin(['else', 'nonexistent']))
complex_filter_with_collection = ((FilterField(SCHEMA.col_a) == "something1") & (FilterField(SCHEMA.col_b).isin(["else", "special"])) & (FilterField(SCHEMA.col_c) == 123))

# Apply the filters for Astra
with AstraClient(
        client_name='test',
        keyspace='tmp',
        secure_connect_bundle_bytes="base64 bundle string",
        client_id='client id',
        client_secret='client secret'
) as ac:
    # Filter expressions are compiled into specific target, in this case Astra filters, in filter_entities method
    print(ac.filter_entities(TestEntityNew, simple_filter))
    
    # simple filter field == value    
    #         col_a      col_b  col_c      col_d
    # 0  something1  different    456  [1, 2, 3]
    # 1  something1       else    123     [1, 2]    
    
    print(ac.filter_entities(TestEntityNew, combined_filter))

    #         col_a col_b  col_c   col_d
    # 0  something1  else    123  [1, 2]

    print(ac.filter_entities(TestEntityNew, combined_filter_with_collection))

    #         col_a col_b  col_c
    # 0  something1  else    123

   print(ac.filter_entities(TestEntityNew, complex_filter_with_collection))
    #         col_a col_b  col_c
    # 0  something1  else    123
  ```

## Using the Vector Search
```python
from adapta.storage.distributed_object_store.datastax_astra.astra_client import AstraClient
from adapta.storage.distributed_object_store.datastax_astra import SimilarityFunction

from dataclasses import dataclass, field

@dataclass
class TestEntityWithEmbeddings:
    col_a: str = field(metadata={
        "is_primary_key": True,
        "is_partition_key": True
    })
    col_b: str = field(metadata={
        "is_primary_key": True,
        "is_partition_key": False
    })
    col_c: list[float] = field(metadata={
        "is_vector_enabled": True
    })


# Apply the filters for Astra
with AstraClient(
        client_name='test',
        keyspace='tmp',
        secure_connect_bundle_bytes="base64 bundle string",
        client_id='client id',
        client_secret='client secret'
) as ac:
    # Filter expressions are compiled into specific target, in this case Astra filters, in filter_entities method
    print(ac.ann_search(entity_type=TestEntityWithEmbeddings, vector_to_match=[0.1, 0.2, 0.3], similarity_function=SimilarityFunction.DOT_PRODUCT, num_results=2))
       
    #         col_a      col_b  col_c              sim_value
    # 0  something1  different    [0.3, 0.4, 0.5]  123.123
    # 1  something2  different1   [0.1, 0.24, 0.25] 456.789
  ```