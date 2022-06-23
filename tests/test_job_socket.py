import copy

import pytest

from proteus.connectors.beast import JobSocket


def _assert_socket_is_equal(socket_1: JobSocket, socket_2: JobSocket):
    assert socket_1.alias == socket_2.alias
    assert socket_1.data_path == socket_2.data_path
    assert socket_1.data_format == socket_2.data_format
    assert isinstance(socket_1, JobSocket)
    assert isinstance(socket_2, JobSocket)


def test_job_socket_serialization():
    socket = JobSocket(alias='foo', data_path='bar', data_format='baz')
    socket_deserialized = JobSocket.deserialize(socket.serialize())

    _assert_socket_is_equal(socket, socket_deserialized)


def test_socket_from_list():

    foo_socket = JobSocket(alias='foo', data_path='foo_path', data_format='foo_format')
    bar_socket = JobSocket(alias='bar', data_path='bar_path', data_format='bar_format')
    baz_socket = JobSocket(alias='baz', data_path='baz_path', data_format='baz_format')
    sockets = [foo_socket, bar_socket, baz_socket]

    _assert_socket_is_equal(foo_socket, JobSocket.from_list(sockets, 'foo'))
    _assert_socket_is_equal(bar_socket, JobSocket.from_list(sockets, 'bar'))
    _assert_socket_is_equal(baz_socket, JobSocket.from_list(sockets, 'baz'))

    sockets.append(copy.deepcopy(foo_socket))
    with pytest.raises(ValueError):
        JobSocket.from_list(sockets, 'foo')

    with pytest.raises(ValueError):
        JobSocket.from_list(sockets, 'non-existing')
