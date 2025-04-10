import pytest


@pytest.fixture(scope="class")
def rootpath(request: pytest.FixtureRequest) -> None:
    """Records the rootpath at the class level, for use on a unittest.TestCase."""
    request.cls._rootpath = request.config._rootpath
