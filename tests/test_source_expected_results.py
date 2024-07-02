import pytest

from tests.hakai_ids_with_issues import HAKAI_IDS_WITH_ISSUES


@pytest.fixture(scope="module")
def source(request):
    return request.config.getoption("--test-suite-from")


@pytest.fixture(scope="module")
def df_test_suite(source, df_initial, df_local):
    if source == "local":
        # Review locally processed data
        return df_local
    # Review original dat from server
    return df_initial


@pytest.mark.parametrize("hakai_id", HAKAI_IDS_WITH_ISSUES["do_cap_fail_hakai_ids"])
def test_hakai_id_with_do_cap_issue(hakai_id, df_test_suite, source):
    data = df_test_suite.loc[df_test_suite["hakai_id"] == hakai_id]

    assert not data.empty, f"{source} No data found for hakai_id {hakai_id}"
    assert (
        data["dissolved_oxygen_ml_l_flag_level_1"] == 4
    ).all(), f"{source} dissolved_oxygen_ml_l_flag_level_1 for hakai_id {hakai_id} is not 4=FAIL: {data['dissolved_oxygen_ml_l_flag_level_1'].unique().tolist()}"
    assert (
        data["dissolved_oxygen_ml_l_flag"].str.startswith("SVD").all()
    ), f"{source} dissolved_oxygen_ml_l_flag for hakai_id {hakai_id} is not flagged as SVD"
    assert (
        data["dissolved_oxygen_ml_l_flag"].str.contains("do_cap_test")
    ).all(), f"{source} do_cap_test not in dissolved_oxygen_ml_l_flag for hakai_id {hakai_id}"


@pytest.mark.parametrize("hakai_id", HAKAI_IDS_WITH_ISSUES["no_soak_warning_hakai_ids"])
def test_hakai_ids_with_no_soak_warning(hakai_id, df_test_suite, source):
    data = df_test_suite.loc[df_test_suite["hakai_id"] == hakai_id]
    for variable in [
        "dissolved_oxygen_ml_l",
        "temperature",
        "salinity",
        "conductivity",
    ]:
        assert not data.empty, f"{source} No data found for hakai_id {hakai_id}"
        assert (
            data[f"{variable}_flag_level_1"] == 3
        ).all(), (
            f"{source} {variable}_flag_level_1 for hakai_id {hakai_id} is not 3=WARNING"
        )
        assert (
            data[f"{variable}_flag"].str.startswith("SVC").all()
        ), f"{source} {variable}_flag for hakai_id {hakai_id} is not flagged as SVC"
        assert (
            data[f"{variable}_flag"].str.contains("no_soak_test")
        ).all(), f"{source} no_soak_test not in {variable}_flag for hakai_id {hakai_id}"


@pytest.mark.parametrize(
    "hakai_id", HAKAI_IDS_WITH_ISSUES["slow_oxygen_warning_hakai_ids"]
)
def test_hakai_ids_with_slow_oxygen_warning(hakai_id, df_test_suite, source):
    data = df_test_suite.loc[df_test_suite["hakai_id"] == hakai_id]

    assert not data.empty, f"{source} No data found for hakai_id {hakai_id}"
    assert (
        data["dissolved_oxygen_ml_l_flag_level_1"] == 3
    ).all(), f"{source} dissolved_oxygen_ml_l_flag_level_1 for hakai_id {hakai_id} is not 3=WARNING"
    assert (
        data["dissolved_oxygen_ml_l_flag"].str.startswith("SVC").all()
    ), f"{source} dissolved_oxygen_ml_l_flag for hakai_id {hakai_id} is not flagged as SVC"
    assert (
        data["dissolved_oxygen_ml_l_flag"].str.contains("hakai_slow_oxygen_sensor_test")
    ).all(), f"{source} hakai_slow_oxygen_sensor_test not in dissolved_oxygen_ml_l_flag for hakai_id {hakai_id}"


@pytest.mark.parametrize("hakai_id", HAKAI_IDS_WITH_ISSUES["short_static_deployment"])
def test_hakai_ids_with_short_static_deployment(hakai_id, df_test_suite, source):
    data = df_test_suite.loc[df_test_suite["hakai_id"] == hakai_id]

    assert not data.empty, f"{source} No data found for hakai_id {hakai_id}"
    assert (
        data["dissolved_oxygen_ml_l_flag_level_1"] == 3
    ).all(), f"{source} dissolved_oxygen_ml_l_flag_level_1 for hakai_id {hakai_id} is not 3=WARNING"
    assert (
        data["dissolved_oxygen_ml_l_flag"].str.startswith("SVC").all()
    ), f"{source} dissolved_oxygen_ml_l_flag for hakai_id {hakai_id} is not flagged as SVC"
    assert (
        data["dissolved_oxygen_ml_l_flag"].str.contains("short_static_deployment_test")
    ).all(), f"{source} short_static_deployment_test not in dissolved_oxygen_ml_l_flag for hakai_id {hakai_id}"
