import os

import pandas as pd
from hakai_api import Client

from hakai_profile_qc.__main__ import read_config_yaml

config = read_config_yaml()
MODULE_PATH = os.path.dirname(__file__)


def get_hakai_test_suite_data_locally():
    client = Client()
    query = (
        client.api_root
        + "/ctd/views/file/cast/data?hakai_id={"
        + ",".join(config["TEST_HAKAI_IDS"])
        + "}&fields="
        + ",".join(config["CTD_CAST_DATA_VARIABLES"])
        + "&limit=-1"
    )
    response = client.get(query)
    pd.DataFrame(response.json()).to_parquet(
        f"{MODULE_PATH}/test_data/ctd_test_suite.parquet"
    )


if __name__ == "__main__":
    # Update local test suite data
    get_hakai_test_suite_data_locally()
