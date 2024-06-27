from pathlib import Path

import pandas as pd
import pytest
from loguru import logger

from tests.utils import get_hakai_test_suite_data, get_hakai_test_suite_metadata

MODULE_PATH = Path(__file__).parent


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


@pytest.fixture(scope="module")
def source(request):
    return request.config.getoption("--test-suite-from")


@pytest.fixture(scope="module")
def df_initial(source):
    if source == "local":
        logger.debug("Fetching test suite from local")
        df = pd.read_parquet(MODULE_PATH / "test_data" / "ctd_test_suite.parquet")
    elif source in ("hecate","goose"):
        df = get_hakai_test_suite_data(source)
    else:
        raise ValueError(
            f"Invalid source '{source}' for the test suite. Must be one of 'local', 'hecate', 'goose'"
        )

    return df.set_index("ctd_data_pk").copy()


@pytest.fixture(scope="module")
def df_local_metadata(source):
    if source == "local":
        return pd.read_parquet(
            MODULE_PATH / "test_data" / "ctd_test_suite_metadata.parquet"
        )
    elif source in ("hecate","goose"):
        return get_hakai_test_suite_metadata(api_root="hecate")
    else:
        raise ValueError(
            f"Invalid source '{source}' for the test suite. Must be one of 'local', 'hecate', 'goose'"
        )
