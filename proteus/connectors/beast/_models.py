"""
  Models for Beast connector
"""
import os
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import List, Dict, Optional, Union

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

    def to_utils_format(self) -> str:
        """
         Converts JobSocket
        :return:
        """
        return f"{self.alias}|{self.data_path}|{self.data_format}"


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

    def to_json(self) -> Dict:
        """
         Converts this to POST body sent to Beast.
        :return:
        """
        base_request = {
            "rootPath": self.root_path,
            "projectName": self.project_name,
            "version": self.version,
            "runnable": self.runnable,
            "inputs": list(map(lambda js: {
                "alias": js.alias,
                "dataPath": js.data_path,
                "dataFormat": js.data_format
            }, self.inputs)),
            "outputs": list(map(lambda js: {
                "alias": js.alias,
                "dataPath": js.data_path,
                "dataFormat": js.data_format
            }, self.outputs)),
            "overwrite": self.overwrite,
            "extraArgs": self.extra_args,
            "clientTag": self.client_tag
        }

        if self.cost_optimized:
            base_request.setdefault("costOptimized", self.cost_optimized)

        if self.job_size:
            base_request.setdefault("jobSize", self.job_size.name)

        if self.flexible_driver:
            base_request.setdefault("flexibleDriver", self.flexible_driver)

        return base_request


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
            raise ValueError(
                'Encryption key not found, but a value is set to be encrypted. Either disable encryption or map RUNTIME_ENCRYPTION_KEY on this container from airflow secrets.')

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


@dataclass
class BeastJobParams:
    """
     Parameters for Beast jobs.
    """
    project_name: str = field(metadata={
        'description': 'Repository name that contains a runnable. Must be deployed to a Beast-managed cluster beforehand.'})
    project_version: str = field(metadata={
        'description': 'Semantic version of a runnable.'})
    project_runnable: str = field(metadata={
        'description': 'Path to a runnable, for example src/folder1/my_script.py.'})
    project_inputs: List[JobSocket] = field(metadata={
        'description': 'List of job inputs.'})
    project_outputs: List[JobSocket] = field(metadata={
        'description': 'List of job outputs.'})
    overwrite_outputs: bool = field(metadata={
        'description': 'Whether to wipe existing data before writing new out.'})
    extra_arguments: Dict[str, Union[ArgumentValue, str]] = field(metadata={
        'description': 'Extra arguments for a submission, defined by an author.'})
    size_hint: Optional[JobSize] = field(metadata={
        'description': 'Job size hint for Beast.'})
    cost_optimized: Optional[bool] = field(metadata={
        'description': 'Job will run on a discounted workload (spot capacity).'})
    flexible_driver: Optional[bool] = field(metadata={
        'description': 'Whether to use fixed-size driver or derive driver memory from master node max memory.'},
        default=False)
