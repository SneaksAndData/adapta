"""Common python typing functions. All of these are imported into __init__.py"""

#  Copyright (c) 2023. ECCO Sneaks & Data
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

from typing import Type, get_origin, Union, get_args


def is_optional(type_: Type) -> bool:
    """
    Checks if a type is Optional.

    :param type_: Type to check.
    :return: True if the type is Optional, False otherwise.
    """
    return get_origin(type_) is Union and type(None) in get_args(type_)
