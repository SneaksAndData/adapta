import os
import tempfile

import pytest
from requests import Session

from adapta.utils import LocalFileAdapter


@pytest.fixture
def session_with_local_adapter():
    """Fixture for a requests.Session with LocalFileAdapter mounted."""
    session = Session()
    session.mount("file://", LocalFileAdapter())
    return session


def test_valid_file_get(session_with_local_adapter):
    with tempfile.NamedTemporaryFile("w+b", delete=False) as tmp:
        tmp.write(b"hello world")
        tmp_path = tmp.name

    try:
        response = session_with_local_adapter.get(f"file://{tmp_path}")
        assert response.status_code == 200
        assert response.content == b"hello world"
    finally:
        os.remove(tmp_path)


@pytest.mark.parametrize("method", ["PUT", "DELETE"])
def test_unsupported_methods(session_with_local_adapter, method):
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_path = tmp.name

    try:
        response = session_with_local_adapter.request(method, f"file://{tmp_path}")
        assert response.status_code == 501
    finally:
        os.remove(tmp_path)


@pytest.mark.parametrize("method", ["POST", "PATCH"])
def test_invalid_methods(session_with_local_adapter, method):
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_path = tmp.name

    try:
        response = session_with_local_adapter.request(method, f"file://{tmp_path}")
        assert response.status_code == 405
    finally:
        os.remove(tmp_path)


def test_directory_path(session_with_local_adapter):
    dir_path = tempfile.mkdtemp()
    response = session_with_local_adapter.get(f"file://{dir_path}")
    assert response.status_code == 400


def test_file_not_found(session_with_local_adapter):
    fake_path = "/non/existent/path/file.txt"
    response = session_with_local_adapter.get(f"file://{fake_path}")
    assert response.status_code == 404


def test_unreadable_file(session_with_local_adapter):
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_path = tmp.name
    try:
        os.chmod(tmp_path, 0o000)
        response = session_with_local_adapter.get(f"file://{tmp_path}")
        assert response.status_code == 403
    finally:
        os.chmod(tmp_path, 0o644)
        os.remove(tmp_path)
