from proteus.connectors.beast import JobSocket


def test_job_socket_serialization():
    socket = JobSocket(alias='foo', data_path='bar', data_format='baz')
    socket_deserialized = JobSocket.deserialize(socket.serialize())

    assert socket.alias == socket_deserialized.alias
    assert socket.data_path == socket_deserialized.data_path
    assert socket.data_format == socket_deserialized.data_format
    assert isinstance(socket_deserialized, JobSocket)
