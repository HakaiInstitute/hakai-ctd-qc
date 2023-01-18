import os

from setuptools import setup

version = {}
setup_path = os.path.join(os.path.dirname(__file__))
version_path = os.path.join(setup_path, "hakai_profile_qc", "version.py")
with open(version_path) as version_file:
    exec(version_file.read(), version)

setup(
    name="hakai_profile_qc",
    version=version["__version__"],
    description="Hakai methods used to test qc config a datasets",
    url="https://github.com/HakaiInstitute/hakai-profile-qaqc",
    author="Jessy Barrette",
    author_email="jessy.barrette@hakai.org",
    license="MIT",
    packages=["hakai_profile_qc"],
    include_package_data=True,
    install_requires=[
        "ioos_qc",
        "gsw",
        "pandas",
        "numpy",
        "hakai_api",
        "tqdm",
        "pyyaml",
        "sentry_sdk",
    ],
    zip_safe=False,
)
