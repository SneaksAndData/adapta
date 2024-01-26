#  Copyright (c) 2023-2024. ECCO Sneaks & Data
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

"""
 Wrapper for Python-based schema classes.
"""

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

    def get_field_names(self) -> list[str]:
        """
        Returns the list of field names of the schema instance

        :return: list of field names
        """
        return list(self.__dict__.keys())
