from dataclasses import Field
from typing import Union, Any, List


class PythonSchemaEntity:
    """Entity used to override getattr to provide schema hints"""

    def __init__(self, underlying_type: Union[Any, List[Field]]) -> None:
        for field_name in underlying_type.__dataclass_fields__:
            self.__setattr__(field_name, field_name)

    # We should implement here __getattribute__ explicitly to avoid `no-member` warning from pylint
    def __getattribute__(self, item):
        # pylint: disable=W0235
        return super().__getattribute__(item)
