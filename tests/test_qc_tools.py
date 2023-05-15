import os

from hakai_profile_qc import __main__

MODULE_PATH = os.path.dirname(__file__)


class TestConfig:
    def test_config_load(self):
        config = __main__.read_config_yaml()
        assert isinstance(config, dict), "Configuration is not a dictionary"
        assert config, "Configuration is not available"
