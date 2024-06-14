import os

import pandas as pd
from hakai_api import Client

from hakai_profile_qc.variables import CTD_CAST_DATA_VARIABLES, HAKAI_TEST_SUITE

MODULE_PATH = os.path.dirname(__file__)


def get_hakai_test_suite_data_locally():
    client = Client()
    query = (
        client.api_root
        + "/ctd/views/file/cast/data?hakai_id={"
        + ",".join(HAKAI_TEST_SUITE)
        + "}&fields="
        + ",".join(CTD_CAST_DATA_VARIABLES)
        + "&limit=-1"
    )
    response = client.get(query)
    response.raise_for_status()
    pd.DataFrame(response.json()).to_parquet(
        f"{MODULE_PATH}/test_data/ctd_test_suite.parquet"
    )

def get_hakai_test_suite_metadata_locally():
    client = Client()
    query = (
        client.api_root
        + "/ctd/views/file/cast?hakai_id={"
        + ",".join(HAKAI_TEST_SUITE)
        + "}&limit=-1"
    )
    response = client.get(query)
    response.raise_for_status()
    pd.DataFrame(response.json()).to_parquet(
        f"{MODULE_PATH}/test_data/ctd_test_suite_metadata.parquet"
    )

if __name__ == "__main__":
    # Update local test suite data
    get_hakai_test_suite_data_locally()
    get_hakai_test_suite_metadata_locally()