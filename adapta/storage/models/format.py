"""
Serialization formats for saving data structures as blob.
"""

#  Copyright (c) 2023-2026. ECCO Data & AI and other project contributors.
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

from abc import ABC, abstractmethod
from typing import Generic, TypeVar

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

    @property
    def file_format(self) -> str:
        """
        Returns the file format for the serialization.
        :return: File format for the serialization.
        """
        return ""

    @property
    def append_file_format_extension(self) -> bool:
        """
        Returns whether to include the file format as an extension in the output name.
        :return: True if the file format should be included in the output name, False otherwise.
        """
        return False

    def get_output_name(self, output_name: str):
        """
        Returns the file name for the serialized data and adds the file format if enabled for serializer.
        :param output_name: Name of the file.
        :return: File name for the serialized data.
        """
        return f"{output_name}.{self.file_format}" if self.append_file_format_extension else output_name


Output = TypeVar("Output")
Schema = TypeVar("Schema")


class SchemaBoundSerializationFormat(SerializationFormat[Output, Schema]):
    """
    Abstract serialization format with schema
    """

    def serialize(self, data: Output, **kwargs) -> bytes:
        return self._serialize_with_schema(data, **kwargs)

    @abstractmethod
    def _serialize_with_schema(self, data: Output, schema: Schema, **_) -> bytes:
        """"""

    def deserialize(self, data: bytes, **_) -> Output:
        pass

    @abstractmethod
    def _deserialize_with_schema(self, data: bytes, schema: Schema, **_) -> Output:
        """"""
