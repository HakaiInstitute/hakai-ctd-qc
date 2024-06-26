# conftest.py
def pytest_addoption(parser):
    parser.addoption(
        "--test-suite-from",
        action="store",
        default="local",
        help="Define from which source the test suite is retrieved (local,hecate,goose)",
    )
    parser.addoption(
        "--test-suite-qc",
        action="store",
        default="True",
        help="Define if the test suite is or rely on the already existing QC (True,False)",
    )
