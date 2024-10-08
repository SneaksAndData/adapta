"""
Serialization formats for saving data structures as blob.
"""
#  Copyright (c) 2023-2024. ECCO Sneaks & Data
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

import json
import io
import pickle
from abc import ABC, abstractmethod
from typing import Generic, TypeVar

import pandas
import polars

from adapta.utils.metaframe import MetaFrame

T = TypeVar("T")  # pylint: disable=C0103


class SerializationFormat(ABC, Generic[T]):
    """
    Abstract serialization format.
    """

    @abstractmethod
    def serialize(self, data: T) -> bytes:
        """
        Serializes data to bytes given a format.
        :param data: Data to serialize.
        :return: Serialized data as byte array.
        """

    @abstractmethod
    def deserialize(self, data: bytes) -> T:
        """
        Deserializes data from bytes given a format.
        :param data: Data to deserialize.
        :return: Deserialized data.
        """


class MetaFrameParquetSerializationFormat(SerializationFormat[MetaFrame]):
    """
    Serializes MetaFrames as parquet format. The MetaFrame is converted to Polars DataFrame before serialization.
    """

    def serialize(self, data: MetaFrame) -> bytes:
        """
        Serializes MetaFrame to bytes using parquet format.
        :param data: MetaFrame to serialize.
        :return: Parquet serialized MetaFrame as byte array.
        """
        bytes_object = io.BytesIO()
        data.to_polars().write_parquet(bytes_object)
        return bytes_object.getvalue()

    def deserialize(self, data: bytes) -> MetaFrame:
        """
        Deserializes MetaFrame from bytes using parquet format.
        :param data: MetaFrame to deserialize in parquet format as bytes.
        :return: Deserialized MetaFrame.
        """
        return MetaFrame(
            data=io.BytesIO(data),
            convert_to_polars=polars.read_parquet,
            convert_to_pandas=pandas.read_parquet,
        )


class DataFrameParquetSerializationFormat(SerializationFormat[pandas.DataFrame]):
    """
    Serializes dataframes as parquet format.
    """

    def serialize(self, data: pandas.DataFrame) -> bytes:
        """
        Serializes dataframe to bytes using parquet format.
        :param data: Dataframe to serialize.
        :return: Parquet serialized dataframe as byte array.
        """
        return data.to_parquet()

    def deserialize(self, data: bytes) -> pandas.DataFrame:
        """
        Deserializes dataframe from bytes using parquet format.
        :param data: Dataframe to deserialize in parquet format as bytes.
        :return: Deserialized dataframe.
        """
        return pandas.read_parquet(io.BytesIO(data))


class DataFrameCsvSerializationFormat(SerializationFormat[pandas.DataFrame]):
    """
    Serializes dataframes as CSV format.
    """

    def serialize(self, data: pandas.DataFrame) -> bytes:
        """
        Serializes dataframe to bytes using CSV format.
        :param data: Dataframe to serialize.
        :return: CSV serialized dataframe as byte array.
        """
        return data.to_csv(index=False).encode(encoding="utf-8")

    def deserialize(self, data: bytes) -> pandas.DataFrame:
        """
        Deserializes dataframe from bytes using CSV format.
        :param data: Dataframe to deserialize in CSV format as bytes.
        :return: Deserialized dataframe.
        """
        return pandas.read_csv(io.BytesIO(data))


class PolarsDataFrameParquetSerializationFormat(SerializationFormat[polars.DataFrame]):
    """
    Serializes dataframes as parquet format.
    """

    def serialize(self, data: polars.DataFrame) -> bytes:
        """
        Serializes dataframe to bytes using parquet format.
        :param data: Dataframe to serialize.
        :return: Parquet serialized dataframe as byte array.
        """
        buffer = io.BytesIO()
        data.write_parquet(buffer)
        return buffer.getvalue()

    def deserialize(self, data: bytes) -> polars.DataFrame:
        """
        Deserializes dataframe from bytes using parquet format.
        :param data: Dataframe to deserialize in parquet format as bytes.
        :return: Deserialized dataframe.
        """
        return polars.read_parquet(io.BytesIO(data))


class PolarsDataFrameCsvSerializationFormat(SerializationFormat[polars.DataFrame]):
    """
    Serializes dataframes as CSV format.
    """

    def serialize(self, data: polars.DataFrame) -> bytes:
        """
        Serializes dataframe to bytes using CSV format.
        :param data: Dataframe to serialize.
        :return: CSV serialized dataframe as byte array.
        """

        return data.write_csv().encode(encoding="utf-8")

    def deserialize(self, data: bytes) -> polars.DataFrame:
        """
        Deserializes dataframe from bytes using CSV format.
        :param data: Dataframe to deserialize in CSV format as bytes.
        :return: Deserialized dataframe.
        """
        return polars.read_csv(io.BytesIO(data))


class PolarsDataFrameJsonSerializationFormat(SerializationFormat[polars.DataFrame]):
    """
    Serializes dataframes as JSON format.
    """

    def serialize(self, data: polars.DataFrame) -> bytes:
        """
        Serializes dataframe to bytes using JSON format.
        :param data: Dataframe to serialize.
        :return: JSON serialized dataframe as byte array.
        """
        return data.write_json().encode(encoding="utf-8")

    def deserialize(self, data: bytes) -> polars.DataFrame:
        """
        Deserializes dataframe from bytes using JSON format.
        :param data: Dataframe to deserialize in JSON format as bytes.
        :return: Deserialized dataframe.
        """
        return polars.read_json(io.BytesIO(data))


class PolarsLazyFrameParquetSerializationFormat(SerializationFormat[polars.LazyFrame]):
    """
    Serializes lazyframes as parquet format.
    """

    def serialize(self, data: polars.LazyFrame) -> bytes:
        """
        Serializes lazyframe to bytes using parquet format.
        :param data: Lazyframe to serialize.
        :return: Parquet serialized lazyframe as byte array.
        """
        buffer = io.BytesIO()
        data.collect().write_parquet(buffer)
        return buffer.getvalue()

    def deserialize(self, data: bytes) -> polars.LazyFrame:
        """
        Deserializes lazyframe from bytes using parquet format.
        :param data: Lazyframe to deserialize in parquet format as bytes.
        :return: Deserialized lazyframe.
        """
        return polars.scan_parquet(io.BytesIO(data))


class PolarsLazyFrameCsvSerializationFormat(SerializationFormat[polars.LazyFrame]):
    """
    Serializes lazyframes as CSV format.
    """

    def serialize(self, data: polars.LazyFrame) -> bytes:
        """
        Serializes lazyframe to bytes using CSV format.
        :param data: Lazyframe to serialize.
        :return: CSV serialized Lazyframe as byte array.
        """

        return data.collect().write_csv().encode(encoding="utf-8")

    def deserialize(self, data: bytes) -> polars.LazyFrame:
        """
        Deserializes lazyframe from bytes using CSV format.
        :param data: LazyFrame to deserialize in CSV format as bytes.
        :return: Deserialized lazyframe.
        """
        return polars.scan_csv(io.BytesIO(data))


class PolarsLazyFrameJsonSerializationFormat(SerializationFormat[polars.LazyFrame]):
    """
    Serializes lazyframes as JSON format.
    """

    def serialize(self, data: polars.LazyFrame) -> bytes:
        """
        Serializes lazyframes to bytes using JSON format.
        :param data: LazyFrame to serialize.
        :return: JSON serialized lazyframe as byte array.
        """
        return data.collect().write_ndjson().encode(encoding="utf-8")

    def deserialize(self, data: bytes) -> polars.LazyFrame:
        """
        Deserializes lazyframes from bytes using JSON format.
        :param data: LazyFrame to deserialize in JSON format as bytes.
        :return: Deserialized lazyframe.
        """
        return polars.scan_ndjson(io.BytesIO(data))


class DictJsonSerializationFormat(SerializationFormat[dict]):
    """
    Serializes dictionaries as JSON format.
    """

    def serialize(self, data: dict) -> bytes:
        """
        Serializes dictionary to bytes using JSON format.
        :param data: Dictionary to serialize.
        :return: JSON serialized dictionary as byte array.
        """
        return json.dumps(data).encode(encoding="utf-8")

    def deserialize(self, data: bytes) -> dict:
        """
        Deserializes dictionary from bytes using JSON format.
        :param data: Dictionary to deserialize in JSON format as bytes.
        :return: Deserialized dictionary.
        """
        return json.loads(data.decode("utf-8"))


class DataFrameJsonSerializationFormat(SerializationFormat[pandas.DataFrame]):
    """
    Serializes dataframes as JSON format.
    """

    def serialize(self, data: pandas.DataFrame) -> bytes:
        """
        Serializes dataframe to bytes using JSON format.
        :param data: Dataframe to serialize.
        :return: JSON serialized dataframe as byte array.
        """
        return json.dumps(data.to_dict(orient="records")).encode(encoding="utf-8")

    def deserialize(self, data: bytes) -> pandas.DataFrame:
        """
        Deserializes dataframe from bytes using JSON format.
        :param data: Dataframe to deserialize in JSON format as bytes.
        :return: Deserialized dataframe.
        """
        return pandas.read_json(io.BytesIO(data), orient="records")


class UnitSerializationFormat(SerializationFormat[bytes]):
    """
    Accepts bytes and returns the exact same bytes. I.e. this class provides a unit serialization of bytes.
    """

    def serialize(self, data: bytes) -> bytes:
        """
        Unit serializes bytes to bytes, i.e. returns the exact same byte sequence.
        :param data: Bytes to serialize.
        :return: Serialized bytes.
        """
        return data

    def deserialize(self, data: bytes) -> bytes:
        """
        Unit deserializes bytes to bytes, i.e. returns the exact same byte sequence.
        :param data: Bytes to deserialize.
        :return: Deserialized bytes.
        """
        return data


class PickleSerializationFormat(SerializationFormat[T]):
    """
    Serializes objects as pickle format.
    """

    def serialize(self, data: T) -> bytes:
        """
        Serializes objects to bytes using pickle format.
        :param data: Object to serialize.
        :return: Pickle serialized object as byte array.
        """
        return pickle.dumps(data)

    def deserialize(self, data: bytes) -> T:
        """
        Deserializes objects from bytes using pickle format.
        :param data: Object to deserialize in pickle format as bytes.
        :return: Deserialized object.
        """
        return pickle.loads(data)


# Temporary aliases
PandasDataFrameParquetSerializationFormat = DataFrameParquetSerializationFormat
PandasDataFrameCsvSerializationFormat = DataFrameCsvSerializationFormat
PandasDataFrameJsonSerializationFormat = DataFrameJsonSerializationFormat
