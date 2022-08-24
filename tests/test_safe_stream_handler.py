import sys

from proteus.logs.handlers.safe_stream_handler import SafeStreamHandler


def test_does_not_close_stderr():
    ssh = SafeStreamHandler(sys.stderr)
    ssh.close()
    assert not sys.stderr.closed


def test_does_not_close_stdout():
    ssh = SafeStreamHandler(sys.stdout)
    stream = ssh.stream
    ssh.close()
    assert not sys.stdout.closed
    assert stream.closed
