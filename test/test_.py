import pytest

@pytest.fixture
def some_fixture():
    return "fixture"


def test_python_project(some_fixture):
    print(some_fixture)

    assert True
