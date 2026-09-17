from typing import final


@final
class SchemaOutOfSyncError(BaseException):
    """
     Marker for schema out-of-sync errors.
    """
    def __init__(self, message: str) -> None:
        super().__init__(message)
