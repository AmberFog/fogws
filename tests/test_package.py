from importlib.metadata import version

import fogws


def test_native_and_python_versions_match() -> None:
    assert fogws.__version__ == version("fogws")


def test_public_surface_is_explicit() -> None:
    assert fogws.__all__ == ("__version__",)
