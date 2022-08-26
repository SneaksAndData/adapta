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
