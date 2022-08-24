import os
import sys
from io import TextIOWrapper
from logging import StreamHandler

from typing import IO


class SafeStreamHandler(StreamHandler):
    def __init__(self, stream: IO = None):
        if stream is sys.stdout:
            duplicate = os.dup(stream.fileno())
            file_object = os.fdopen(duplicate, "wb")
            stream = TextIOWrapper(file_object)
        super().__init__(stream=stream)
