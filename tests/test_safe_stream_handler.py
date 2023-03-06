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

import sys

from proteus.logs.handlers.safe_stream_handler import SafeStreamHandler


def test_does_not_close_stderr():
    ssh = SafeStreamHandler(sys.stderr)
    stream = ssh.stream
    ssh.close()
    assert not stream.closed


def test_does_not_close_stdout():
    ssh = SafeStreamHandler(sys.stdout)
    stream = ssh.stream
    ssh.close()
    assert not sys.stdout.closed
    assert stream.closed


def test_replace_stdout():
    ssh = SafeStreamHandler(sys.stdout)
    stream = ssh.stream
    assert stream is not sys.stdout


def test_not_replaces_other_streams():
    ssh = SafeStreamHandler(sys.stderr)
    stream = ssh.stream
    assert stream is sys.stderr
