import os
import sys
from io import TextIOWrapper
from logging import StreamHandler


class StdoutSafeHandler(StreamHandler):
    def __init__(self):
        duplicate = os.dup(sys.stdout.fileno())
        file_ = os.fdopen(duplicate, "wb")
        stream = TextIOWrapper(file_)
        super().__init__(stream=stream)
