import tomllib
from pathlib import Path


def get_version_from_pyproject():
    pyproject = Path(__file__).parent.parent / "pyproject.toml"
    data = tomllib.loads(pyproject.read_text())
    return data["project"]["version"]


__version__ = get_version_from_pyproject()
