"""
  Models for Beast connector
"""
import os
from dataclasses import dataclass
from enum import Enum, auto
from typing import List, Dict, Optional

from cryptography.fernet import Fernet


@dataclass
class JobSocket:
    """
     Input/Output data map

     Attributes:
         alias: mapping key to be used by a consumer
         data_path: fully qualified path to actual data, i.e. abfss://..., s3://... etc.
         data_format: data format, i.e. csv, json, delta etc.
    """
    alias: str
    data_path: str
    data_format: str


class JobSize(Enum):
    """
      Job size hints for Beast.

      TINY - jobs running DDL or hive imports or any tiny workload. Should receive minimum allowed resources (1 core, 1g ram per executor)
      SMALL - small workloads, one or 30% pod cores, 30% pod memory per executor
      MEDIUM - medium workloads, one or 30% pod cores, 50% pod memory per executor
      LARGE - large workloads, one or 30% pod cores, all available executor memory
      XLARGE - extra large workload, two or 60% pod cores, all available executor memory
      XXLARGE - x-extra large workloads, all available cores, all available memory
    """
    TINY = auto()
    SMALL = auto()
    MEDIUM = auto()
    LARGE = auto()
    XLARGE = auto()
    XXLARGE = auto()


@dataclass
class JobRequest:
    """
     Request body for a Beast submission
    """
    root_path: str
    project_name: str
    version: str
    runnable: str
    inputs: List[JobSocket]
    outputs: List[JobSocket]
    overwrite: bool
    extra_args: Dict[str, str]
    client_tag: str
    cost_optimized: Optional[bool]
    job_size: Optional[JobSize]
    flexible_driver: Optional[bool]


class ArgumentValue:
    """
      Wrapper around job argument value. Supports fernet encryption.
    """

    def __init__(self, *, value: str, encrypt=False, quote=False, is_env=False):
        """
          Initializes a new ArgumentValue

        :param value: Plain text value.
        :param encrypt: If set to True, value will be replaced with a fernet-encrypted value.
        :param quote: Whether a value should be quoted when it is stringified.
        :param is_env: whether value should be derived from env instead, using value as var name.
        """
        self._is_env = is_env
        self._encrypt = encrypt
        self._quote = quote
        self._value = value

    @property
    def value(self):
        """
          Returns the wrapped value

        :return:
        """
        if self._is_env:
            result = os.getenv(self._value)
        else:
            result = self._value

        if self._encrypt:
            result = self._encrypt_value(result)

        return result

    @staticmethod
    def _encrypt_value(value: str) -> str:
        """
          Encrypts a provided string

        :param value: payload to decrypt
        :return: Encrypted payload
        """
        encryption_key = os.environ.get('RUNTIME_ENCRYPTION_KEY', None).encode('utf-8')

        if not encryption_key:
            raise ValueError('Encryption key not found, but a value is set to be encrypted. Either disable encryption or map RUNTIME_ENCRYPTION_KEY on this container from airflow secrets.')

        fernet = Fernet(encryption_key)
        return fernet.encrypt(value.encode('utf-8')).decode('utf-8')

    def __str__(self):
        """
         Stringifies the value and optionally wraps it in quotes.

        :return:
        """
        if self._quote:
            return f"'{str(self.value)}'"

        return self.value
