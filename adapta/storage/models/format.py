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

from pandas import DataFrame, read_parquet, read_csv, read_json

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


class DataFrameParquetSerializationFormat(SerializationFormat[DataFrame]):
    """
    Serializes dataframes as parquet format.
    """

    def serialize(self, data: DataFrame) -> bytes:
        """
        Serializes dataframe to bytes using parquet format.
        :param data: Dataframe to serialize.
        :return: Parquet serialized dataframe as byte array.
        """
        return data.to_parquet()

    def deserialize(self, data: bytes) -> DataFrame:
        """
        Deserializes dataframe from bytes using parquet format.
        :param data: Dataframe to deserialize in parquet format as bytes.
        :return: Deserialized dataframe.
        """
        return read_parquet(io.BytesIO(data))


class DataFrameCsvSerializationFormat(SerializationFormat[DataFrame]):
    """
    Serializes dataframes as CSV format.
    """

    def serialize(self, data: DataFrame) -> bytes:
        """
        Serializes dataframe to bytes using CSV format.
        :param data: Dataframe to serialize.
        :return: CSV serialized dataframe as byte array.
        """
        return data.to_csv(index=False).encode(encoding="utf-8")

    def deserialize(self, data: bytes) -> DataFrame:
        """
        Deserializes dataframe from bytes using CSV format.
        :param data: Dataframe to deserialize in CSV format as bytes.
        :return: Deserialized dataframe.
        """
        return read_csv(io.BytesIO(data))


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


class DataFrameJsonSerializationFormat(SerializationFormat[DataFrame]):
    """
    Serializes dataframes as JSON format.
    """

    def serialize(self, data: DataFrame) -> bytes:
        """
        Serializes dataframe to bytes using JSON format.
        :param data: Dataframe to serialize.
        :return: JSON serialized dataframe as byte array.
        """
        return json.dumps(data.to_dict(orient="records")).encode(encoding="utf-8")

    def deserialize(self, data: bytes) -> DataFrame:
        """
        Deserializes dataframe from bytes using JSON format.
        :param data: Dataframe to deserialize in JSON format as bytes.
        :return: Deserialized dataframe.
        """
        return read_json(io.BytesIO(data), orient="records")


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
