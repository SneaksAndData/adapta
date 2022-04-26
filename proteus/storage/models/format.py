"""
Serialization formats for saving data structures as blob.
"""
from abc import ABC, abstractmethod
import pandas as pd
from proteus.storage.blob._functions import json_to_bytes


class DataFrameSerializationFormat(ABC):
    """
    Abstract dataframe serialization format.
    """
    @abstractmethod
    def serialize(self, df: pd.DataFrame) -> bytes:
        """
        Serializes dataframe to bytes given a format.
        :param df: Dataframe to serialize.
        :return: Serialized dataframe as byte array.
        """


class ParquetSerializationFormat(DataFrameSerializationFormat):
    """
    Serializes dataframes as parquet format.
    """
    def serialize(self, df: pd.DataFrame) -> bytes:
        """
        Serializes dataframe to bytes using parquet format.
        :param df: Dataframe to serialize.
        :return: Parquet serialized dataframe as byte array.
        """
        return df.to_parquet()


class JsonSerializationFormat(DataFrameSerializationFormat):
    """
    Serializes dataframes as JSON format.
    """
    def serialize(self, df: pd.DataFrame) -> bytes:
        """
        Serializes dataframe to bytes using JSON format.
        :param df: Dataframe to serialize.
        :return: JSON serialized dataframe as byte array.
        """
        return json_to_bytes(df.to_dict(orient='records'))
