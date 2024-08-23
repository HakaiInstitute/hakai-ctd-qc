import re

from hakai_ctd_qc import variables

hakai_id_regex = r"\d+_\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z"


class TestLoadTestSuite:
    def test_load_hakai_id_list_loader(self):
        hakai_id_test_suite = variables.load_test_suite()
        assert hakai_id_test_suite
        assert not [
            item for item in hakai_id_test_suite if "#" in item
        ], "Hakai ID test suite loader failed to drop comments= # ..."

    def test_hakai_id_test_suite_list(self):
        hakai_ids = variables.HAKAI_TEST_SUITE
        assert [item for item in hakai_ids if not re.fullmatch(hakai_id_regex, item)], (
            "Some items listed in the hakai_id test suite aren't "
            f"matching the expected hakai_id patter={hakai_id_regex}"
        )
