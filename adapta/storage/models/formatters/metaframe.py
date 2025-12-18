"""
Module for serializing and deserializing Metaframe DataFrames in various formats.
"""
import io

import pandas
import polars

from adapta.storage.models.format import SerializationFormat
from adapta.utils.metaframe import MetaFrame


class MetaFrameParquetSerializationFormat(SerializationFormat[MetaFrame]):
    """
    Serializes MetaFrames as parquet format. The MetaFrame is converted to Polars DataFrame before serialization.
    """

    file_format = "parquet"

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


class MetaFrameParquetSerializationFormatWithFileFormat(MetaFrameParquetSerializationFormat):
    """
    Serializes MetaFrames as parquet format with file format.
    """

    append_file_format_extension = True
