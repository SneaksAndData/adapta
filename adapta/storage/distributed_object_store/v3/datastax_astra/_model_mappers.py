"""Model mapper module"""
import datetime
import enum
import sys
import typing
from abc import ABC, abstractmethod
from dataclasses import is_dataclass, fields
from typing import Type, Optional, List
import re

from cassandra.cqlengine.models import Model
from cassandra.cqlengine.columns import Column
from cassandra.cqlengine import columns

TModel = typing.TypeVar("TModel")  # pylint: disable=C0103


class CassandraModelMapper(ABC):
    """
    Abstract class for mapping various data models to Cassandra models.

        :param: keyspace: Optional keyspace name, if not provided in the client constructor.
        :param: table_name: Astra table name, if it cannot be inferred from class name by converting it to snake_case.
        :param: primary_keys: An optional list of columns that constitute a primary key.
        :param: partition_keys: An optional list of columns that constitute a partition key.
        :param: custom_indexes: An optional list of columns that have a custom index on them.
        :param: select_columns: An optional list of columns to select from the entity.
    """

    def __init__(
        self,
        data_model: Type[TModel],
        keyspace: Optional[str] = None,
        table_name: Optional[str] = None,
        primary_keys: Optional[List[str]] = None,
        partition_keys: Optional[List[str]] = None,
        custom_indexes: Optional[List[str]] = None,
        select_columns: Optional[List[str]] = None,
    ):
        self._data_model = data_model
        self._keyspace = keyspace
        self._table_name = table_name
        self._primary_keys = primary_keys
        self._partition_keys = partition_keys
        self._custom_indexes = custom_indexes
        self._select_columns = select_columns
        self._snake_pattern = re.compile(r"(?<!^)(?=[A-Z])")

    def map(
        self,
    ) -> Type[Model]:
        """Maps a datamodel to a Cassandra model."""
        selected_fields = self.get_column_types(subset=self._select_columns)

        models_attributes: typing.Dict[str, typing.Union[Column, str]] = {
            name: self._map_to_cassandra(
                dtype,
                name,
                name in self.primary_keys,
                name in self.partition_keys,
                name in self.custom_indices,
            )
            for name, dtype in selected_fields.items()
        }

        if self._keyspace:
            models_attributes |= {"__keyspace__": self._keyspace}

        return type(self.table_name, (Model,), models_attributes)

    @property
    @abstractmethod
    def table_name(self) -> List[str]:
        """Primary keys for the given data model"""

    @property
    @abstractmethod
    def primary_keys(self) -> List[str]:
        """Primary keys for the given data model"""

    @property
    @abstractmethod
    def partition_keys(
        self,
    ) -> List[str]:
        """Partition keys for the given data model."""

    @property
    @abstractmethod
    def custom_indices(
        self,
    ) -> List[str]:
        """Custom indices for the given data model."""

    @property
    @abstractmethod
    def vector_column(self) -> str:
        """Vector column for the given data model."""

    @abstractmethod
    def get_column_types(
        self,
        subset: List[str],
    ) -> typing.Dict[str, Type]:
        """Get column types for the given data model. If subset is provided, only return types for the subset.

        :param subset: Optional subset of columns to get types for.
        :return: Dictionary of column names and their types.
        """

    @abstractmethod
    def _map_to_column(
        self,
        python_type: Type,
    ) -> typing.Union[
        typing.Tuple[Type[columns.List],],
        typing.Tuple[Type[columns.Map],],
        typing.Tuple[Type[Column],],
        typing.Tuple[Type[Column], Type[Column]],
        typing.Tuple[Type[Column], Type[Column], Type[Column]],
        typing.Tuple[Type[columns.List], columns.Map],
    ]:
        """Map Python type to Cassandra column type.

        :param python_type: Python type to map.
        :return: Cassandra column type.
        """

    def _map_to_cassandra(
        self, python_type: Type, db_field: str, is_primary_key: bool, is_partition_key: bool, is_custom_index: bool
    ) -> Column:
        cassandra_types = self._map_to_column(python_type)
        if len(cassandra_types) == 1:  # simple type
            return cassandra_types[0](
                primary_key=is_primary_key,
                partition_key=is_partition_key,
                db_field=db_field,
                custom_index=is_custom_index,
            )
        if len(cassandra_types) == 2:  # list
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


class DataclassMapper(CassandraModelMapper):
    """Maps dataclasses to Cassandra models."""

    def __init__(
        self,
        data_model: Type[TModel],
        keyspace: Optional[str] = None,
        table_name: Optional[str] = None,
        primary_keys: Optional[List[str]] = None,
        partition_keys: Optional[List[str]] = None,
        custom_indexes: Optional[List[str]] = None,
        select_columns: Optional[List[str]] = None,
    ):
        super().__init__(
            data_model=data_model,
            keyspace=keyspace,
            table_name=table_name,
            primary_keys=primary_keys,
            partition_keys=partition_keys,
            custom_indexes=custom_indexes,
            select_columns=select_columns,
        )

    @property
    def table_name(self) -> str:
        return self._table_name or self._snake_pattern.sub("_", self._data_model.__name__).lower()

    @property
    def primary_keys(self) -> List[str]:
        return self._primary_keys or [
            field.name for field in fields(self._data_model) if field.metadata.get("is_primary_key", False)
        ]

    @property
    def partition_keys(
        self,
    ) -> List[str]:
        return self._partition_keys or [
            field.name for field in fields(self._data_model) if field.metadata.get("is_partition_key", False)
        ]

    @property
    def custom_indices(
        self,
    ) -> List[str]:
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

    def get_column_types(
        self,
        subset: Optional[List[str]] = None,
    ) -> typing.Dict[str, Type]:
        return {field.name: field.type for field in fields(self._data_model) if not subset or field.name in subset}

    def _map_to_column(  # pylint: disable=R0911
        self,
        python_type: Type,
    ) -> typing.Union[
        typing.Tuple[Type[columns.List],],
        typing.Tuple[Type[columns.Map],],
        typing.Tuple[Type[Column],],
        typing.Tuple[Type[Column], Type[Column]],
        typing.Tuple[Type[Column], Type[Column], Type[Column]],
        typing.Tuple[Type[columns.List], columns.Map],
    ]:
        if python_type is type(None):
            raise TypeError("NoneType cannot be mapped to any existing table column types")
        if python_type is bool:
            return (columns.Boolean,)
        if python_type is str:
            return (columns.Text,)
        if python_type is bytes:
            return (columns.Blob,)
        if python_type is datetime.datetime:
            return (columns.DateTime,)
        if python_type is int:
            return (columns.Integer,)
        if python_type is float:
            return (columns.Double,)
        if (
            sys.version_info.minor > 9 and type(python_type) is enum.EnumType  # pylint: disable=unidiomatic-typecheck
        ) or (
            sys.version_info.minor <= 9 and type(python_type) is enum.EnumMeta  # pylint: disable=unidiomatic-typecheck
        ):  # assume all enums are strings - for now
            return (columns.Text,)
        if typing.get_origin(python_type) == list:
            args = typing.get_args(python_type)
            if typing.get_origin(args[0]) == dict:
                dict_args = typing.get_args(args[0])
                return (
                    columns.List,
                    columns.Map(
                        self._map_to_column(dict_args[0])[0],
                        self._map_to_column(dict_args[1])[0],
                    ),
                )
            return (
                columns.List,
                self._map_to_column(typing.get_args(python_type)[0])[0],
            )
        if typing.get_origin(python_type) == dict:
            return (
                columns.Map,
                self._map_to_column(typing.get_args(python_type)[0])[0],
                self._map_to_column(typing.get_args(python_type)[1])[0],
            )

        if typing.get_origin(python_type) == typing.Union:
            return self._map_to_column(typing.get_args(python_type)[0])

        raise TypeError(f"Unsupported type: {python_type}")


def model_mapper_factory(
    data_model: Type[TModel],
    keyspace: Optional[str] = None,
    table_name: Optional[str] = None,
    primary_keys: Optional[List[str]] = None,
    partition_keys: Optional[List[str]] = None,
    custom_indexes: Optional[List[str]] = None,
    select_columns: Optional[List[str]] = None,
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
            select_columns=select_columns,
        )
    raise TypeError(f"Unsupported data model type: {data_model}")
