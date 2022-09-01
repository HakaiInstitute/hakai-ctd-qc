from setuptools import setup

setup(
    name="hakai_profile_qc",
    version="0.0.2",
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
        "pyproj",
        "pandas",
        "numpy",
        "hakai_api",
        "tqdm",
        "pyyaml",
        "sentry_sdk",
    ],
    zip_safe=False,
)
