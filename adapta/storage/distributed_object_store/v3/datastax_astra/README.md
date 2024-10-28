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
from adapta.storage.distributed_object_store.v3.datastax_astra import AstraClient

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
        client_id='Astra Token client_id',
        client_secret='Astra Token client_secret'
) as ac:
    single_entity = ac.get_entity('test_entity')
    print(single_entity)
    # {'col_a': 'something3', 'col_b': 'ordinal', 'col_c': 'today'}

    multiple_entities = ac.get_entities_raw("select * from tmp.test_entity where col_a = 'something3'").to_pandas()
    print(multiple_entities)
    #    col_a    col_b  col_c
    # 0  something3  ordinal  today

    print(ac.filter_entities(TestEntity, key_column_filter_values=[{"col_a": 'something1'}]).to_pandas())
    #        col_a col_b     col_c
    #0  something1  else  entirely

    print(ac.filter_entities(TestEntity, key_column_filter_values=[{"col_a": 'something1'}],
                             select_columns=['col_c']).to_pandas())
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
from adapta.storage.distributed_object_store.v3.datastax_astra import AstraClient
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
combined_filter_with_collection = (FilterField(SCHEMA.col_a) == "something1") & (
    FilterField(SCHEMA.col_b).isin(['else', 'nonexistent']))

# Apply the filters for Astra
with AstraClient(
        client_name='test',
        keyspace='tmp',
        secure_connect_bundle_bytes="base64 bundle string",
        client_id='client id',
        client_secret='client secret'
) as ac:
    # Filter expressions are compiled into specific target, in this case Astra filters, in filter_entities method
    print(ac.filter_entities(TestEntityNew, simple_filter).to_pandas())

    # simple filter field == value    
    #         col_a      col_b  col_c
    # 0  something1  different    456
    # 1  something1       else    123

    print(ac.filter_entities(TestEntityNew, combined_filter).to_pandas())
    #         col_a col_b  col_c
    # 0  something1  else    123
    
    print(ac.filter_entities(TestEntityNew, combined_filter_with_collection).to_pandas())
    #         col_a col_b  col_c
    # 0  something1  else    123
  ```

## Using the Vector Search
1. Create a table in Astra and insert some rows:
```cassandraql
CREATE TABLE IF NOT EXISTS tmp.test_entity_with_embeddings (
    col_a TEXT PRIMARY KEY,
    col_b TEXT,
    col_c VECTOR<FLOAT, 3>,
    col_d TEXT,
);

CREATE INDEX IF NOT EXISTS ann_index
  ON tmp.test_entity_with_embeddings(col_c)
  WITH OPTIONS = {'source_model': 'other'};

CREATE INDEX IF NOT EXISTS col_b_index
  ON tmp.test_entity_with_embeddings(col_b);

INSERT INTO tmp.test_entity_with_embeddings (col_a, col_b, col_c, col_d)
VALUES ('something1', 'different', [0.3, 0.4, 0.5], 'extra1');

INSERT INTO tmp.test_entity_with_embeddings (col_a, col_b, col_c, col_d)
VALUES ('something2', 'different1', [0.1, 0.24, 0.25], 'extra2');
```
2. Test out functionality in Python
```python
from adapta.storage.distributed_object_store.v3.datastax_astra import AstraClient
from adapta.storage.distributed_object_store.v3.datastax_astra import SimilarityFunction
from adapta.storage.models.filter_expression import FilterField

from dataclasses import dataclass, field


@dataclass
class TestEntityWithEmbeddings:
    col_a: str = field(metadata={
        "is_primary_key": True,
        "is_partition_key": True
    })
    col_b: str
    col_c: list[float] = field(metadata={
        "is_vector_enabled": True
    })
    col_d: str


astra_client = AstraClient(
    client_name='test',
    keyspace='tmp',
    secure_connect_bundle_bytes='base64string',
    client_id='Astra Token client_id',
    client_secret='Astra Token client_secret'
)

# Search in Astra
with astra_client:
    print(astra_client.ann_search(
        entity_type=TestEntityWithEmbeddings,
        vector_to_match=[0.1, 0.2, 0.3],
        similarity_function=SimilarityFunction.DOT_PRODUCT
        , num_results=2
    ).to_pandas())

    #         col_a       col_b   col_d  sim_value
    # 0  something2  different1  extra2     0.5665
    # 1  something1   different  extra1     0.6300

    
# Search with primary key filter in Astra (with dictionary)
filter_expression = [{'col_a': 'something2', 'col_b': 'different1'}]
with astra_client:
    print(astra_client.ann_search(
        entity_type=TestEntityWithEmbeddings,
        vector_to_match=[0.1, 0.2, 0.3],
        similarity_function=SimilarityFunction.DOT_PRODUCT,
        num_results=2,
        key_column_filter_values=filter_expression
    ).to_pandas())
    
    #         col_a       col_b   col_d  sim_value
    # 0  something2  different1  extra2     0.5665


# Search with primary key filter in Astra (with Expression)
filter_expression = (FilterField('col_a') == 'something2') & (FilterField('col_b').isin(['different1', 'doesnt_exist']))
with astra_client:
    print(astra_client.ann_search(
        entity_type=TestEntityWithEmbeddings,
        vector_to_match=[0.1, 0.2, 0.3],
        similarity_function=SimilarityFunction.DOT_PRODUCT,
        num_results=2,
        key_column_filter_values=filter_expression,
    ).to_pandas())

    #         col_a       col_b   col_d  sim_value
    # 0  something2  different1  extra2     0.5665
  ```


2. Test complex types in python and insert into astra
Create a table in Astra and insert some rows:
```cassandraql
CREATE TABLE tmp.test_entity
(
    column_a                         text PRIMARY KEY,
    column_b                         list<text>,
    column_c                         list<frozen<map<text, double>>>,
)
```

Instantiate a new client, map dataclass (model) to Cassandra model and add rows it:

```python
from adapta.storage.distributed_object_store.v3.datastax_astra import AstraClient

from dataclasses import dataclass, field

@dataclass
class TestEntity:

    column_a: str = field(
        metadata={
            "is_primary_key": True,
            "is_partition_key": True,
        }
    )
    column_b: list[str]
    column_c: list[dict[str, float]]

rows_to_insert = [
    {'column_a': '1', 'column_b': [], 'column_c': [{'key_a': 1, 'key_b': 2}]},
    {'column_a': '2', 'column_b': ['1', '3', '4'], 'column_c': [{'key_a': 1, 'key_b': 2}, {'key_a': 3, 'key_b': 4}]}
]

with AstraClient(
        client_name='test',
        keyspace='tmp',
        secure_connect_bundle_bytes='base64string',
        client_id='Astra Token client_id',
        client_secret='Astra Token client_secret'
) as ac:
    ac.upsert_batch(
    entities=rows_to_insert,
    entity_type=TestEntity,
    keyspace="tmp",
    table_name="test_entity",
    )
```