"""
Module providing a unit serialization format for bytes.
"""
from adapta.storage.models.format import SerializationFormat


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
