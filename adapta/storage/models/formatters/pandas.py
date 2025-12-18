"""
Module for serializing and deserializing pandas DataFrames in various formats.
"""
import io
import json

import pandas

from adapta.storage.models.format import SerializationFormat


class PandasDataFrameJsonSerializationFormat(SerializationFormat[pandas.DataFrame]):
    """
    Serializes dataframes as JSON format.
    """

    file_format = "json"

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


class PandasDataFrameJsonSerializationFormatWithFileFormat(PandasDataFrameJsonSerializationFormat):
    """
    Serializes dataframes as JSON format with file format.
    """

    append_file_format_extension = True


class PandasDataFrameCsvSerializationFormat(SerializationFormat[pandas.DataFrame]):
    """
    Serializes pandas dataframes as CSV format.
    """

    file_format = "csv"

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


class PandasDataFrameCsvSerializationFormatWithFileFormat(PandasDataFrameCsvSerializationFormat):
    """
    Serializes dataframes as CSV format with file format.
    """

    append_file_format_extension = True


class PandasDataFrameParquetSerializationFormat(SerializationFormat[pandas.DataFrame]):
    """
    Serializes pandas dataframes as parquet format.
    """

    file_format = "parquet"

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


class PandasDataFrameParquetSerializationFormatWithFileFormat(PandasDataFrameParquetSerializationFormat):
    """
    Serializes dataframes as parquet format with file format.
    """

    append_file_format_extension = True


class PandasDataFrameExcelSerializationFormat(SerializationFormat[pandas.DataFrame]):
    """
    Serializes dataframes as Excel (.xlsx) format.
    """

    file_format = "xlsx"

    def serialize(self, data: pandas.DataFrame) -> bytes:
        """
        Serializes dataframe to bytes using Excel format.
        :param data: Dataframe to serialize.
        :return: Excel serialized dataframe as byte array.
        """
        buffer = io.BytesIO()
        data.to_excel(buffer, index=False, engine="openpyxl")
        return buffer.getvalue()

    def deserialize(self, data: bytes) -> pandas.DataFrame:
        """
        Deserializes dataframe from bytes using Excel format.
        :param data: Dataframe to deserialize in Excel format as bytes.
        :return: Deserialized dataframe.
        """
        return pandas.read_excel(io.BytesIO(data), engine="openpyxl")


class PandasDataFrameExcelSerializationFormatWithFileFormat(PandasDataFrameExcelSerializationFormat):
    """
    Serializes dataframes as Excel (.xlsx) format with file format extension.
    """

    append_file_format_extension = True
