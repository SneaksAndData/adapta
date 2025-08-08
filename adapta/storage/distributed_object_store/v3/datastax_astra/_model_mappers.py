"""Model mapper module"""
import datetime
import enum
import sys
import typing
from abc import ABC, abstractmethod
from dataclasses import is_dataclass, fields
import re

import polars
import pandera.polars
from cassandra.cqlengine.models import Model
from cassandra.cqlengine.columns import Column
from cassandra.cqlengine import columns

TModel = typing.TypeVar("TModel")  # pylint: disable=C0103


class CassandraModelMapper(ABC):
    """
    Abstract class for mapping various data models to Cassandra models.

        :param: keyspace: Optional keyspace name, if not provided in the client constructor.
        :param: table_name: Cassandra table name, if it cannot be inferred from data model
        :param: primary_keys: An optional list of columns that constitute a primary key. If not provided, it will be inferred from the data model, typically through metadata.
        :param: partition_keys: An optional list of columns that constitute a partition key. If not provided, it will be inferred from the data model, typically through metadata.
        :param: custom_indexes: An optional list of columns that have a custom index on them. If not provided, it will be inferred from the data model, typically through metadata.
    """

    def __init__(
        self,
        data_model: type[TModel],
        keyspace: str | None = None,
        table_name: str | None = None,
        primary_keys: list[str] | None = None,
        partition_keys: list[str] | None = None,
        custom_indexes: list[str] | None = None,
    ):
        self._data_model = data_model
        self._keyspace = keyspace
        self._table_name = table_name
        self._primary_keys = primary_keys
        self._partition_keys = partition_keys
        self._custom_indexes = custom_indexes
        self._snake_pattern = re.compile(r"(?<!^)(?=[A-Z])")

    def map(
        self,
    ) -> type[Model]:
        """Maps a datamodel to a Cassandra model."""
        models_attributes: dict[str, Column | str] = {
            name: self._map_to_cassandra(
                type_to_map=dtype,
                db_field=name,
                is_primary_key=name in self.primary_keys,
                is_partition_key=name in self.partition_keys,
                is_custom_index=name in self.custom_indices,
            )
            for name, dtype in self._get_original_types().items()
        }

        if self._keyspace:
            models_attributes |= {"__keyspace__": self._keyspace}

        return type(self.table_name, (Model,), models_attributes)

    @property
    @abstractmethod
    def column_names(self) -> list[str]:
        """Returns list of column names for the given data model."""

    @property
    @abstractmethod
    def table_name(self) -> str:
        """Primary keys for the given data model"""

    @property
    @abstractmethod
    def primary_keys(self) -> list[str]:
        """Primary keys for the given data model"""

    @property
    @abstractmethod
    def partition_keys(
        self,
    ) -> list[str]:
        """Partition keys for the given data model."""

    @property
    @abstractmethod
    def custom_indices(
        self,
    ) -> list[str]:
        """Custom indices for the given data model."""

    @property
    @abstractmethod
    def vector_column(self) -> str:
        """Vector column for the given data model."""

    @abstractmethod
    def _get_original_types(
        self,
        subset: list[str] | None = None,
    ) -> dict[str, type]:
        """Get original column types for the given data model. If subset is provided, only return types for the subset.

        :param subset: Optional subset of columns to get types for.
        :return: Dictionary of column names and their types.
        """

    def _map_to_column(  # pylint: disable=R0911
        self,
        type_to_map: type,
    ) -> (
        tuple[type[columns.List],]
        | tuple[type[columns.Map],]
        | tuple[type[Column],]
        | tuple[type[Column], type[Column]]
        | tuple[type[Column], type[Column], type[Column]]
        | tuple[type[columns.List], tuple[type[columns.Map]]]
    ):
        """Map Type to Cassandra column type.

        :param type_to_map: Type to map.
        :return: Cassandra column type.
        """
        # pylint: disable=too-many-branches
        if type_to_map is type(None):
            raise TypeError("NoneType cannot be mapped to any existing table column types")
        if type_to_map is bool:
            return (columns.Boolean,)
        if type_to_map is str:
            return (columns.Text,)
        if type_to_map is bytes:
            return (columns.Blob,)
        if type_to_map is datetime.datetime:
            return (columns.DateTime,)
        if type_to_map is int:
            return (columns.Integer,)
        if type_to_map is float:
            return (columns.Double,)
        if (
            sys.version_info.minor > 9 and type(type_to_map) is enum.EnumType  # pylint: disable=unidiomatic-typecheck
        ) or (
            sys.version_info.minor <= 9 and type(type_to_map) is enum.EnumMeta  # pylint: disable=unidiomatic-typecheck
        ):  # assume all enums are strings - for now
            return (columns.Text,)
        if typing.get_origin(type_to_map) == list:
            list_element_type = typing.get_args(type_to_map)[0]
            if typing.get_origin(list_element_type) == dict:
                return (
                    columns.List,
                    self._map_to_column(list_element_type),
                )
            return (
                columns.List,
                self._map_to_column(list_element_type)[0],
            )
        if typing.get_origin(type_to_map) == set:
            list_element_type = typing.get_args(type_to_map)[0]
            if typing.get_origin(list_element_type) == dict:
                return (
                    columns.Set,
                    self._map_to_column(list_element_type),
                )
            return (
                columns.Set,
                self._map_to_column(list_element_type)[0],
            )

        if typing.get_origin(type_to_map) == dict:
            return (
                columns.Map,
                self._map_to_column(typing.get_args(type_to_map)[0])[0],
                self._map_to_column(typing.get_args(type_to_map)[1])[0],
            )

        if typing.get_origin(type_to_map) == typing.Union:
            return self._map_to_column(typing.get_args(type_to_map)[0])

        raise TypeError(f"Unsupported type: {type_to_map}")

    def _map_to_cassandra(
        self, type_to_map: type, db_field: str, is_primary_key: bool, is_partition_key: bool, is_custom_index: bool
    ) -> Column:
        cassandra_types = self._map_to_column(type_to_map)
        if len(cassandra_types) == 1:  # simple type
            return cassandra_types[0](
                primary_key=is_primary_key,
                partition_key=is_partition_key,
                db_field=db_field,
                custom_index=is_custom_index,
            )
        if len(cassandra_types) == 2:  # list
            # if it is a tuple, we are trying to map a dict
            if isinstance(cassandra_types[1], tuple):
                cassandra_map = cassandra_types[1]
                return cassandra_types[0](
                    primary_key=is_primary_key,
                    partition_key=is_partition_key,
                    db_field=db_field,
                    value_type=cassandra_map[0](
                        primary_key=is_primary_key,
                        partition_key=is_partition_key,
                        db_field=db_field,
                        key_type=cassandra_map[1],
                        value_type=cassandra_map[2],
                    ),
                    custom_index=is_custom_index,
                )
            # else it is simple types
            return cassandra_types[0](
                primary_key=is_primary_key,
                partition_key=is_partition_key,
                db_field=db_field,
                value_type=cassandra_types[1],
                custom_index=is_custom_index,
            )
        if len(cassandra_types) == 3:  # dict
            return cassandra_types[0](
                primary_key=is_primary_key,
                partition_key=is_partition_key,
                db_field=db_field,
                key_type=cassandra_types[1],
                value_type=cassandra_types[2],
                custom_index=is_custom_index,
            )

        raise TypeError(f"Unsupported type mapping: {cassandra_types}")


@typing.final
class DataclassMapper(CassandraModelMapper):
    """Maps dataclasses to Cassandra models."""

    def __init__(
        self,
        data_model: type[TModel],
        keyspace: str | None = None,
        table_name: str | None = None,
        primary_keys: list[str] | None = None,
        partition_keys: list[str] | None = None,
        custom_indexes: list[str] | None = None,
    ):
        super().__init__(
            data_model=data_model,
            keyspace=keyspace,
            table_name=table_name,
            primary_keys=primary_keys,
            partition_keys=partition_keys,
            custom_indexes=custom_indexes,
        )

    @property
    def table_name(self) -> str:
        return self._table_name or self._snake_pattern.sub("_", self._data_model.__name__).lower()

    @property
    def column_names(self) -> list[str]:
        return [field.name for field in fields(self._data_model)]

    @property
    def primary_keys(self) -> list[str]:
        return self._primary_keys or [
            field.name for field in fields(self._data_model) if field.metadata.get("is_primary_key", False)
        ]

    @property
    def partition_keys(
        self,
    ) -> list[str]:
        return self._partition_keys or [
            field.name for field in fields(self._data_model) if field.metadata.get("is_partition_key", False)
        ]

    @property
    def custom_indices(
        self,
    ) -> list[str]:
        return self._custom_indexes or [
            field.name for field in fields(self._data_model) if field.metadata.get("is_custom_index", False)
        ]

    @property
    def vector_column(self) -> str:
        vector_columns = [
            field.name for field in fields(self._data_model) if field.metadata.get("is_vector_enabled", False)
        ]

        assert not len(vector_columns) > 1, (
            f"Only a single vector column is allowed in data models. This model"
            f" contains {len(vector_columns)} vector columns: {vector_columns}."
        )
        assert not len(vector_columns) < 1, "No vector column found in the data model"

        return vector_columns[0]

    def _get_original_types(
        self,
        subset: list[str] | None = None,
    ) -> dict[str, type]:
        return {field.name: field.type for field in fields(self._data_model) if not subset or field.name in subset}


@typing.final
class PanderaPolarsMapper(CassandraModelMapper):
    """Maps Pandera Polars data models to Cassandra models."""

    def __init__(
        self,
        data_model: type[pandera.polars.DataFrameModel],
        keyspace: str | None = None,
        table_name: str | None = None,
        primary_keys: list[str] | None = None,
        partition_keys: list[str] | None = None,
        custom_indexes: list[str] | None = None,
    ):
        super().__init__(
            data_model=data_model,
            keyspace=keyspace,
            table_name=table_name,
            primary_keys=primary_keys,
            partition_keys=partition_keys,
            custom_indexes=custom_indexes,
        )
        self._data_model_schema = data_model.to_schema()

    def _map_to_column(
        self,
        type_to_map: type,
    ) -> (
        tuple[type[columns.List],]
        | tuple[type[columns.Map],]
        | tuple[type[Column],]
        | tuple[type[Column], type[Column]]
        | tuple[type[Column], type[Column], type[Column]]
        | tuple[type[columns.List], columns.Map]
    ):
        mapping = {
            polars.Int8: (columns.TinyInt,),
            polars.Int16: (columns.SmallInt,),
            polars.Int32: (columns.Integer,),
            polars.Int64: (columns.BigInt,),
            polars.UInt64: (columns.VarInt,),
            polars.UInt32: (columns.VarInt,),
            polars.UInt16: (columns.VarInt,),
            polars.UInt8: (columns.VarInt,),
            polars.Float64: (columns.Double,),
            polars.Float32: (columns.Float,),
            polars.Boolean: (columns.Boolean,),
            polars.String: (columns.Text,),
            polars.Utf8: (columns.Text,),
            polars.List: (columns.List,),
            polars.List(str): (columns.List, columns.Text),
            polars.List(int): (columns.List, columns.Integer),
            polars.List(float): (columns.List, columns.Float),
            polars.List(bool): (columns.List, columns.Boolean),
            polars.Date: (columns.Date,),
            polars.Datetime: (columns.DateTime,),
            polars.Datetime(time_unit="us"): (columns.DateTime,),
            polars.Datetime(time_unit="ns"): (columns.DateTime,),
            polars.Datetime(time_unit="ms"): (columns.DateTime,),
            polars.Datetime(time_unit="ms", time_zone="UTC"): (columns.DateTime,),
            polars.Datetime(time_unit="us", time_zone="UTC"): (columns.DateTime,),
        }

        column_type = mapping.get(type_to_map, None)

        if column_type is None:
            return super()._map_to_column(type_to_map)

        return column_type

    def _get_original_types(
        self,
        subset: list[str] | None = None,
    ) -> dict[str, type]:
        cols = [col for name, col in self._data_model_schema.columns.items() if not subset or name in subset]
        map_ = {}
        for col in cols:
            column_type = (col.metadata or {}).get("python_type", None) or col.dtype.type
            if column_type is polars.Object:
                raise ValueError(
                    f"Column '{col.name}' is of type polars.Object, which is not supported. Please specify the python type in the metadata."
                )
            map_[col.name] = column_type

        return map_

    @property
    def column_names(self) -> list[str]:
        return list(self._data_model_schema.columns.keys())

    @property
    def table_name(self) -> str:
        return self._table_name or self._data_model.Config.name

    @property
    def primary_keys(self) -> list[str]:
        return self._primary_keys or [
            name
            for name, col in self._data_model_schema.columns.items()
            if col.metadata is not None and col.metadata.get("is_primary_key", False)
        ]

    @property
    def partition_keys(self) -> list[str]:
        return self._partition_keys or [
            name
            for name, col in self._data_model_schema.columns.items()
            if col.metadata is not None and col.metadata.get("is_partition_key", False)
        ]

    @property
    def custom_indices(self) -> list[str]:
        return self._custom_indexes or [
            name
            for name, col in self._data_model_schema.columns.items()
            if col.metadata is not None and col.metadata.get("is_custom_index", False)
        ]

    @property
    def vector_column(self) -> str:
        vector_columns = [
            name
            for name, col in self._data_model_schema.columns.items()
            if col.metadata is not None and col.metadata.get("is_vector_enabled", False)
        ]

        assert not len(vector_columns) > 1, (
            f"Only a single vector column is allowed in data models. This model"
            f" contains {len(vector_columns)} vector columns: {vector_columns}."
        )
        assert not len(vector_columns) < 1, "No vector column found in the data model"

        return vector_columns[0]


def get_mapper(
    data_model: type[TModel],
    keyspace: str | None = None,
    table_name: str | None = None,
    primary_keys: list[str] | None = None,
    partition_keys: list[str] | None = None,
    custom_indexes: list[str] | None = None,
) -> CassandraModelMapper:
    """Factory function for creating a model mapper based on the data model type."""
    if is_dataclass(data_model):
        return DataclassMapper(
            data_model=data_model,
            keyspace=keyspace,
            table_name=table_name,
            primary_keys=primary_keys,
            partition_keys=partition_keys,
            custom_indexes=custom_indexes,
        )

    if issubclass(data_model, pandera.polars.DataFrameModel):
        return PanderaPolarsMapper(
            data_model=data_model,
            keyspace=keyspace,
            table_name=table_name,
            primary_keys=primary_keys,
            partition_keys=partition_keys,
            custom_indexes=custom_indexes,
        )

    raise TypeError(f"Unsupported data model type: {data_model}")
