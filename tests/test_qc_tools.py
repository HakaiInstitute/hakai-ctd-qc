import os

import numpy as np
import pandas as pd
import pytest

from hakai_profile_qc import __main__

MODULE_PATH = os.path.dirname(__file__)
df = pd.read_parquet(f"{MODULE_PATH}/test_data/ctd_test_suite.parquet")


class TestConfig:
    def test_config_load(self):
        config = __main__.read_config_yaml()
        assert isinstance(config, dict), "Configuration is not a dictionary"
        assert config, "Configuration is not available"
