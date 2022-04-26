"""
Serialization formats for saving data structures as blob.
"""
import json
from abc import ABC, abstractmethod
import pandas


class DataFrameSerializationFormat(ABC):
    """
    Abstract dataframe serialization format.
    """
    @abstractmethod
    def serialize(self, p_df: pandas.DataFrame) -> bytes:
        """
        Serializes dataframe to bytes given a format.
        :param p_df: Dataframe to serialize.
        :return: Serialized dataframe as byte array.
        """


class ParquetSerializationFormat(DataFrameSerializationFormat):
    """
    Serializes dataframes as parquet format.
    """
    def serialize(self, p_df: pandas.DataFrame) -> bytes:
        """
        Serializes dataframe to bytes using parquet format.
        :param p_df: Dataframe to serialize.
        :return: Parquet serialized dataframe as byte array.
        """
        return p_df.to_parquet()


class JsonSerializationFormat(DataFrameSerializationFormat):
    """
    Serializes dataframes as JSON format.
    """
    def serialize(self, p_df: pandas.DataFrame) -> bytes:
        """
        Serializes dataframe to bytes using JSON format.
        :param p_df: Dataframe to serialize.
        :return: JSON serialized dataframe as byte array.
        """
        return json.dumps(p_df.to_dict(orient='records')).encode(encoding='utf-8')
