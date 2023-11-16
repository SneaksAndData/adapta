"""
Contains credentials provider for AWS clients
"""
import os
from abc import ABC, abstractmethod


class AccessKeyCredentials(ABC):
    """
    Abstract class that represents credentials for AWS connections
    """

    @property
    @abstractmethod
    def access_key(self) -> str:
        """AWS account access key"""

    @property
    @abstractmethod
    def access_key_id(self) -> str:
        """AWS account access key id"""

    @property
    @abstractmethod
    def region(self) -> str:
        """AWS region"""


class EnvironmentAwsCredentials(AccessKeyCredentials):
    """
    Reads credentials from environment variables
    """

    def __init__(self):
        if "PROTEUS_AWS_SECRET_ACCESS_KEY" not in os.environ:
            raise ValueError("AWS_SECRET_ACCESS_KEY must be set")
        self._access_key = os.environ["PROTEUS_AWS_SECRET_ACCESS_KEY"]

        if "PROTEUS_AWS_ACCESS_KEY_ID" not in os.environ:
            raise ValueError("AWS_ACCESS_ID must be set")
        self._access_key_id = os.environ["PROTEUS_AWS_ACCESS_KEY_ID"]

        if "PROTEUS_AWS_REGION" not in os.environ:
            raise ValueError("AWS_REGION must be set")
        self._region = os.environ["PROTEUS_AWS_REGION"]

    @property
    def access_key(self) -> str:
        return self._access_key

    @property
    def access_key_id(self) -> str:
        return self._access_key_id

    @property
    def region(self) -> str:
        return self.region


class ExplicitAwsCredentials(AccessKeyCredentials):
    """
    Explicitly passed AWS credentials
    """

    def __init__(self, access_key, access_key_id, region):
        self._access_key = access_key
        self._access_key_id = access_key_id
        self._region = region

    @property
    def access_key(self) -> str:
        return self._access_key

    @property
    def access_key_id(self) -> str:
        return self._access_key_id

    @property
    def region(self) -> str:
        return self._region
