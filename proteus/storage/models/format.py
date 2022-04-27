"""
Serialization formats for saving data structures as blob.
"""
import json
import io
from abc import ABC, abstractmethod
from typing import Generic, TypeVar
import pandas

T = TypeVar('T')  # pylint: disable=C0103


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
        return json.dumps(data).encode(encoding='utf-8')

    def deserialize(self, data: bytes) -> dict:
        """
        Deserializes dictionary from bytes using JSON format.
        :param data: Dictionary to deserialize in JSON format as bytes.
        :return: Deserialized dictionary.
        """
        return json.loads(data.decode('utf-8'))


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
        return json.dumps(data.to_dict(orient='records')).encode(encoding='utf-8')

    def deserialize(self, data: bytes) -> pandas.DataFrame:
        """
        Deserializes dataframe from bytes using JSON format.
        :param data: Dataframe to deserialize in JSON format as bytes.
        :return: Deserialized dataframe.
        """
        return pandas.read_json(io.BytesIO(data), orient='records')
