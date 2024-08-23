from pathlib import Path

from hakai_ctd_qc.version import __version__


def test_pyproject_version():
    pyproject = Path(__file__).parent.parent / "pyproject.toml"
    assert pyproject.exists(), "pyproject.toml not found"
    pyproject_text = pyproject.read_text()
    assert (
        f'version = "{__version__}"' in pyproject_text
    ), f"__version__={__version__} not found in pyproject.toml"


def test_changelog():
    changelog = Path(__file__).parent.parent / "CHANGELOG.md"
    assert changelog.exists(), "CHANGELOG.md not found"
    changelog_text = changelog.read_text()
    assert (
        f"## v{__version__}" in changelog_text
    ), f"__version__={__version__} not found in CHANGELOG.md"
