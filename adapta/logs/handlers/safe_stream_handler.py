"""
Logging handler for Stdout that does not create duplicates if stdout redirection is used
"""
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

import os
import sys
from io import TextIOWrapper
from logging import StreamHandler

from typing import IO, Optional


class SafeStreamHandler(StreamHandler):
    """
    Logging handler for Stdout that does not create duplicates if stdout redirection is used
    """

    def __init__(self, stream: Optional[IO] = None):
        """
        Crates new instance
        :param stream: underlying file-like object. If sys.stdout is passed here, it will be reopened
        """
        self.stream: Optional[IO] = None
        self._need_close = False
        if stream is sys.stdout:
            duplicate = os.dup(stream.fileno())
            file_object = os.fdopen(duplicate, "wb")
            stream = TextIOWrapper(file_object)
            self._need_close = True
        super().__init__(stream=stream)

    def close(self):
        """
        Closes the stream.
        """
        if not self._need_close:
            return
        self.acquire()
        try:
            try:
                if self.stream:
                    try:
                        self.flush()
                    finally:
                        stream = self.stream
                        self.stream = None
                        if hasattr(stream, "close"):
                            stream.close()
            finally:
                # Issue #19523: call unconditionally to
                # prevent a handler leak when delay is set
                StreamHandler.close(self)
        finally:
            self.release()
