"""Serialization format for Pickle."""
import pickle

from adapta.storage.models.format import SerializationFormat, T


class PickleSerializationFormat(SerializationFormat[T]):
    """
    Serializes objects as pickle format.
    """

    file_format = "pkl"

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


class PickleSerializationFormatWithFileFormat(PickleSerializationFormat):
    """
    Serializes objects as pickle format with file format.
    """

    append_file_format_extension = True
