"""
Module for serializing and deserializing polars DataFrames in various formats.
"""
import io

import polars

from adapta.storage.models.format import SerializationFormat


class PolarsLazyFrameJsonSerializationFormat(SerializationFormat[polars.LazyFrame]):
    """
    Serializes lazyframes as JSON format.
    """

    file_format = "json"

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


class PolarsLazyFrameJsonSerializationFormatWithFileFormat(PolarsLazyFrameJsonSerializationFormat):
    """
    Serializes lazyframes as JSON format with file format.
    """

    append_file_format_extension = True


class PolarsLazyFrameCsvSerializationFormat(SerializationFormat[polars.LazyFrame]):
    """
    Serializes lazyframes as CSV format.
    """

    file_format = "csv"

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


class PolarsLazyFrameCsvSerializationFormatWithFileFormat(PolarsLazyFrameCsvSerializationFormat):
    """
    Serializes lazyframes as CSV format with file format.
    """

    append_file_format_extension = True


class PolarsLazyFrameParquetSerializationFormat(SerializationFormat[polars.LazyFrame]):
    """
    Serializes lazyframes as parquet format.
    """

    file_format = "parquet"

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


class PolarsLazyFrameParquetSerializationFormatWithFileFormat(PolarsLazyFrameParquetSerializationFormat):
    """
    Serializes lazyframes as parquet format with file format.
    """

    append_file_format_extension = True


class PolarsDataFrameJsonSerializationFormat(SerializationFormat[polars.DataFrame]):
    """
    Serializes dataframes as JSON format.
    """

    file_format = "json"

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


class PolarsDataFrameJsonSerializationFormatWithFileFormat(PolarsDataFrameJsonSerializationFormat):
    """
    Serializes dataframes as JSON format with file format.
    """

    append_file_format_extension = True


class PolarsDataFrameCsvSerializationFormat(SerializationFormat[polars.DataFrame]):
    """
    Serializes dataframes as CSV format.
    """

    file_format = "parquet"

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


class PolarsDataFrameCsvSerializationFormatWithFileFormat(PolarsDataFrameCsvSerializationFormat):
    """
    Serializes dataframes as CSV format with file format.
    """

    append_file_format_extension = True


class PolarsDataFrameParquetSerializationFormat(SerializationFormat[polars.DataFrame]):
    """
    Serializes dataframes as parquet format.
    """

    file_format = "parquet"

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


class PolarsDataFrameParquetSerializationFormatWithFileFormat(PolarsDataFrameParquetSerializationFormat):
    """
    Serializes dataframes as parquet format with file format.
    """

    append_file_format_extension = True


class PolarsDataFrameExcelSerializationFormat(SerializationFormat[polars.DataFrame]):
    """
    Serializes dataframes as Excel (.xlsx) format.
    """

    file_format = "xlsx"

    def serialize(self, data: polars.DataFrame) -> bytes:
        """
        Serializes dataframe to bytes using Excel format.
        :param data: Dataframe to serialize.
        :return: Excel serialized dataframe as byte array.
        """
        buffer = io.BytesIO()
        data.write_excel(buffer)
        return buffer.getvalue()

    def deserialize(self, data: bytes) -> polars.DataFrame:
        """
        Deserializes dataframe from bytes using Excel format.
        :param data: Dataframe to deserialize in Excel format as bytes.
        :return: Deserialized dataframe.
        """
        return polars.read_excel(io.BytesIO(data))


class PolarsDataFrameExcelSerializationFormatWithFileFormat(PolarsDataFrameExcelSerializationFormat):
    """
    Serializes dataframes as Excel (.xlsx) format with file format.
    """

    append_file_format_extension = True
