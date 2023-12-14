"""
 Functions for handling environment settings and configuration reads.
"""
import os
from typing import Optional


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

LEGACY_ROOT_PREFIX = "PROTEUS"
ROOT_PREFIX = "ADAPTA"


def get_domain_environment_variable(name: str, default_value: Optional[str] = None) -> Optional[str]:
    """
    Returns a value of environment variable bound to ADAPTA or PROTEUS (legacy) domain.
    """
    domain_bound_name = f"{ROOT_PREFIX}__{name.upper()}"
    legacy_domain_bound_name = f"{ROOT_PREFIX}__{name.upper()}"

    return os.getenv(domain_bound_name, default_value) or os.getenv(legacy_domain_bound_name, default_value)


def set_domain_environment_variable(name: str, value: str) -> None:
    """
    Set a value of environment variable bound to ADAPTA or PROTEUS (legacy) domain.
    """
    domain_bound_name = f"{ROOT_PREFIX}__{name.upper()}"
    legacy_domain_bound_name = f"{ROOT_PREFIX}__{name.upper()}"

    os.environ.setdefault(domain_bound_name, value)
    os.environ.setdefault(legacy_domain_bound_name, value)
