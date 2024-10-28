from dataclasses import dataclass, field
from typing import Type

import polars
import pytest
import pandera.polars
from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model
from pandera.typing import Series

from adapta.storage.distributed_object_store.v3.datastax_astra._model_mappers import (
    DataclassMapper,
    CassandraModelMapper,
    get_mapper,
    PanderaPolarsMapper,
)

cols = {"text_column": columns.Text(primary_key=True)}


class CassandraModel(Model):
    __table_name__ = "test_table"
    first_name = columns.Text(primary_key=True)
    country = columns.Text(primary_key=True, partition_key=True)
    last_name = columns.Text()
    age = columns.Integer()
    skills = columns.Map(key_type=columns.Text, value_type=columns.Text)
    likes_cake = columns.Boolean()
    nicknames = columns.List(value_type=columns.Text)


@dataclass
class DataclassModel:
    first_name: str = field(metadata={"is_primary_key": True})
    country: str = field(metadata={"is_primary_key": True, "is_partition_key": True})
    last_name: str
    age: int
    skills: dict[str, str]
    likes_cake: bool
    nicknames: list[str]


@dataclass
class DataclassModelWithoutMetadata:
    first_name: str
    country: str
    last_name: str
    age: int
    skills: dict[str, str]
    likes_cake: bool
    nicknames: list[str]


class PanderaPolarsModel(pandera.polars.DataFrameModel):
    first_name: str = pandera.polars.Field(metadata={"is_primary_key": True})
    country: str = pandera.polars.Field(metadata={"is_primary_key": True, "is_partition_key": True})
    last_name: str
    age: Series[
        polars.Int32
    ]  # Polars maps python native int to Int64, requiring explicit typing for Cassandra Compliance
    skills: object = pandera.polars.Field(metadata={"python_type": dict[str, str]})
    likes_cake: bool
    nicknames: list[str]

    class Config:
        name = "test_table"


@pytest.mark.parametrize(
    "data_model, Mapper, mapper_kwargs",
    [
        (
            DataclassModelWithoutMetadata,
            DataclassMapper,
            {"table_name": "test_table", "primary_keys": ["first_name", "country"], "partition_keys": ["country"]},
        ),
        (DataclassModel, DataclassMapper, {"table_name": "test_table"}),
        (DataclassModel, DataclassMapper, {}),
        (PanderaPolarsModel, PanderaPolarsMapper, {}),
    ],
)
def test_cassandra_model_mapper(data_model, Mapper: Type[CassandraModelMapper], mapper_kwargs: dict):
    mapper = Mapper(data_model=data_model, **mapper_kwargs)
    mapped_model = mapper.map()
    assert (
        sorted(mapper.primary_keys) == sorted(CassandraModel._primary_keys.keys()) == sorted(mapped_model._primary_keys)
    )

    assert (
        sorted(CassandraModel._partition_keys.keys())
        == sorted(mapper.partition_keys)
        == sorted(mapped_model._partition_keys)
    )

    assert sorted(CassandraModel._columns.keys()) == sorted(mapper.column_names) == sorted(mapped_model._columns.keys())
    assert {c: type(i) for c, i in CassandraModel._columns.items()} == {
        c: type(i) for c, i in mapped_model._columns.items()
    }


@pytest.mark.parametrize(
    "data_model, expected_mapper",
    [
        (DataclassModel, DataclassMapper),
        (DataclassModelWithoutMetadata, DataclassMapper),
        (PanderaPolarsModel, PanderaPolarsMapper),
    ],
)
def test_model_mapper_factory(data_model, expected_mapper: Type[CassandraModelMapper]):
    mapper = get_mapper(data_model)

    assert isinstance(mapper, expected_mapper)
