"""
Models for log messages
"""

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

from dataclasses import dataclass
from typing import Optional, Dict


@dataclass
class CompositeLogMetadata:
    """
    Metadata model for log messages created by CompositeLogger
    """

    template: str
    diagnostics: Optional[str] = None
    tags: Optional[Dict[str, str]] = None
    fields: Optional[Dict[str, str]] = None
