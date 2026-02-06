from typing import Self


class IcebergRestCatalog:
    def __init__(self):
        pass

    @classmethod
    def create(cls, uri: str, warehouse: str) -> Self:
        pass

    @property
    def get_constructor_args(self) -> dict:
        return {}
