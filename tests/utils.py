import os

import pandas as pd
from hakai_api import Client

from hakai_profile_qc.variables import (CTD_CAST_DATA_VARIABLES,
                                        HAKAI_TEST_SUITE)

MODULE_PATH = os.path.dirname(__file__)


def define_api_root(api_root=None):
    if api_root == "hecate":
        return "https://hecate.hakai.org/api"
    elif api_root == "goose":
        return "https://goose.hakai.org/api"
    return api_root


def get_hakai_test_suite_data(api_root=None):
    client = Client()
    root = define_api_root(api_root) or client.api_root
    query = (
        root
        + "/ctd/views/file/cast/data?hakai_id={"
        + ",".join(HAKAI_TEST_SUITE)
        + "}&fields="
        + ",".join(CTD_CAST_DATA_VARIABLES)
        + "&limit=-1"
    )
    response = client.get(query)
    response.raise_for_status()
    return pd.DataFrame(response.json())


def get_hakai_test_suite_data_locally():
    df = get_hakai_test_suite_data()
    df.to_parquet(f"{MODULE_PATH}/test_data/ctd_test_suite.parquet")


def get_hakai_test_suite_metadata(api_root=None):
    client = Client()
    root = define_api_root(api_root) or client.api_root
    query = (
        root
        + "/ctd/views/file/cast?hakai_id={"
        + ",".join(HAKAI_TEST_SUITE)
        + "}&limit=-1"
    )
    response = client.get(query)
    response.raise_for_status()
    return pd.DataFrame(response.json())


def get_hakai_test_suite_metadata_locally():
    df = get_hakai_test_suite_metadata()
    df.to_parquet(f"{MODULE_PATH}/test_data/ctd_test_suite_metadata.parquet")


if __name__ == "__main__":
    # Update local test suite data
    get_hakai_test_suite_data_locally()
    get_hakai_test_suite_metadata_locally()
