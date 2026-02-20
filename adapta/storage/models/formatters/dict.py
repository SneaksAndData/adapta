"""
Module for serializing and deserializing dictionaries.
"""

import json

from adapta.storage.models.format import SerializationFormat
from adapta.storage.models.formatters.exceptions import SerializationError


class DictJsonSerializationFormat(SerializationFormat[dict]):
    """
    Serializes dictionaries as JSON format.
    """

    file_format = "json"

    def serialize(self, data: dict, **_) -> bytes:
        """
        Serializes dictionary to bytes using JSON format.
        :param data: Dictionary to serialize.
        :return: JSON serialized dictionary as byte array.
        """
        return json.dumps(data).encode(encoding="utf-8")

    def deserialize(self, data: bytes, **_) -> dict:
        """
        Deserializes dictionary from bytes using JSON format.
        :param data: Dictionary to deserialize in JSON format as bytes.
        :return: Deserialized dictionary.
        """
        result = json.loads(data.decode("utf-8"))
        if isinstance(result, dict):
            return result

        raise SerializationError(f"Deserialized data is not a dictionary. Actual type: {type(result)}")


class DictJsonSerializationFormatWithFileFormat(DictJsonSerializationFormat):
    """
    Serializes dictionaries as JSON format with file format.
    """

    append_file_format_extension = True
