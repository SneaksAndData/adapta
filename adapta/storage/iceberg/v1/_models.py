"""
 Iceberg REST wrapper
"""
import os
from dataclasses import dataclass
from typing import Self


@dataclass
class IcebergRestOAuth2Config:
    """
    OAuth2 connection config for Iceberg REST catalog
    """

    credential: str
    server_uri: str
    scope: str

    @classmethod
    def from_environment(cls) -> Self:
        """
        Initialize config from environment
        """
        try:
            return cls(
                os.environ["ADAPTA__ICEBERG_REST_CATALOG_OAUTH__CREDENTIAL"],
                os.environ["ADAPTA__ICEBERG_REST_CATALOG_OAUTH__URI"],
                os.environ["ADAPTA__ICEBERG_REST_CATALOG_OAUTH__SCOPE"],
            )
        except KeyError as ke:
            raise RuntimeError(
                "Missing a required environment variable. Must have: ADAPTA__ICEBERG_REST_CATALOG_OAUTH__CREDENTIAL, ADAPTA__ICEBERG_REST_CATALOG_OAUTH__URI, ADAPTA__ICEBERG_REST_CATALOG_OAUTH__SCOPE"
            ) from ke


class IcebergRestCatalogConfig:
    """
    Iceberg REST catalog connection configuration
    """

    def __init__(self, uri: str, warehouse: str, oauth2config: IcebergRestOAuth2Config):
        self._uri = uri
        self._warehouse = warehouse
        self._oauth2config = oauth2config

    @classmethod
    def create(cls, uri: str, warehouse: str, oauth2config: IcebergRestOAuth2Config) -> Self:
        """
        Construct this config.
        """
        return cls(uri, warehouse, oauth2config)

    @classmethod
    def from_environment(cls) -> Self:
        """
        Constructs this config using data from environment variables only
        """
        return cls(
            uri=os.environ["ADAPTA__ICEBERG_REST_CATALOG_URI"],
            warehouse=os.environ["ADAPTA__ICEBERG_REST_CATALOG_WAREHOUSE"],
            oauth2config=IcebergRestOAuth2Config.from_environment(),
        )

    @property
    def get_constructor_args(self) -> dict:
        """
        Generate arguments for `load_catalog`
        """
        return {
            "type": "rest",
            "uri": self._uri,
            "warehouse": self._warehouse,
            "header.X-Iceberg-Access-Delegation": "vended-credentials",
            "credential": self._oauth2config.credential,
            "oauth2-server-uri": self._oauth2config.server_uri,
            "scope": self._oauth2config.scope,
        }
