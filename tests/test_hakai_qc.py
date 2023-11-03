import pandas as pd

from hakai_profile_qc.__main__ import _get_hakai_flag_columns

df = pd.DataFrame(
    {
        "salinity": [1, 2, 3, 4, 5, 6],
        "salinity_qartod_flag_1": [1, 1, 1, 1, 1, 1],
        "salinity_qartod_flag_2": [1, 1, 1, 1, 1, 1],
    }
)
var = "salinity"


class TestHakaiFlags:
    def test_hakai_flag_all_good(self):
        df_result = _get_hakai_flag_columns(df, var, r"_qartod_flag")
        assert (df_result["salinity_flag_level_1"] == 1).all()
        assert (df_result["salinity_flag"].str.startswith("AV")).all()

    def test_hakai_flag_all_good_with_unknown(self):
        df_temp = df.copy()
        df_temp["salinity_qartod_flag_1"].iloc[2] = 2
        df_temp = _get_hakai_flag_columns(df_temp, var, r"_qartod_flag")
        assert (df_temp["salinity_flag_level_1"] == 1).all()
        assert (df_temp["salinity_flag"].str.startswith("AV")).all()

    def test_hakai_flag_all_good_with_suspect(self):
        df_temp = df.copy()
        df_temp["salinity_qartod_flag_1"].iloc[0] = 3
        df_temp = _get_hakai_flag_columns(df_temp, var, r"_qartod_flag")
        assert df_temp["salinity_flag_level_1"].iloc[0] == 3
        assert (df_temp["salinity_flag_level_1"].iloc[1:] == 1).all()
        assert df_temp["salinity_flag"].iloc[0].startswith("SVC")
        assert (df_temp["salinity_flag"].iloc[1:].str.startswith("AV")).all()

    def test_hakai_flag_all_good_with_fail(self):
        df_temp = df.copy()
        df_temp["salinity_qartod_flag_1"].iloc[0] = 4
        df_temp = _get_hakai_flag_columns(df_temp, var, r"_qartod_flag")
        assert df_temp["salinity_flag_level_1"].iloc[0] == 4
        assert (df_temp["salinity_flag_level_1"].iloc[1:] == 1).all()
        assert df_temp["salinity_flag"].iloc[0].startswith("SVD")
        assert (df_temp["salinity_flag"].iloc[1:].str.startswith("AV")).all()

    def test_hakai_flag_all_good_with_fail_and_suspect(self):
        df_temp = df.copy()
        df_temp["salinity_qartod_flag_1"].iloc[0] = 4
        df_temp["salinity_qartod_flag_2"].iloc[0] = 3
        df_temp = _get_hakai_flag_columns(df_temp, var, r"_qartod_flag")
        assert df_temp["salinity_flag_level_1"].iloc[0] == 4
        assert (df_temp["salinity_flag_level_1"].iloc[1:] == 1).all()
        assert df_temp["salinity_flag"].iloc[0].startswith("SVD")
        assert "SVC: " in df_temp["salinity_flag"].iloc[0]
        assert (df_temp["salinity_flag"].iloc[1:].str.startswith("AV")).all()

    def test_hakai_flag_all_good_with_suspect_and_fail(self):
        df_temp = df.copy()
        df_temp["salinity_qartod_flag_1"].iloc[0] = 3
        df_temp["salinity_qartod_flag_2"].iloc[0] = 4
        df_temp = _get_hakai_flag_columns(df_temp, var, r"_qartod_flag")
        assert df_temp["salinity_flag_level_1"].iloc[0] == 4
        assert (df_temp["salinity_flag_level_1"].iloc[1:] == 1).all()
        assert df_temp["salinity_flag"].iloc[0].startswith("SVD")
        assert "SVC: " in df_temp["salinity_flag"].iloc[0]
        assert (df_temp["salinity_flag"].iloc[1:].str.startswith("AV")).all()

    def test_hakai_flag_all_good_with_fail_and_suspect_ignore_other_flag(self):
        df_temp = df.copy()
        df_temp["salinity_qartod_flag_1"].iloc[0] = 4
        df_temp["salinity_qartod_flag_2"].iloc[0] = 3
        df_temp = _get_hakai_flag_columns(df_temp, var, r"_qartod_flag_1")
        assert df_temp["salinity_flag_level_1"].iloc[0] == 4
        assert (df_temp["salinity_flag_level_1"].iloc[1:] == 1).all()
        assert df_temp["salinity_flag"].iloc[0].startswith("SVD")
        assert "SVC: " not in df_temp["salinity_flag"].iloc[0]
        assert (df_temp["salinity_flag"].iloc[1:].str.startswith("AV")).all()

    def test_hakai_flag_all_good_with_fail_and_null_value(self):
        df_temp = df.copy()
        df_temp["salinity_qartod_flag_1"].iloc[0] = 4
        df_temp["salinity"].iloc[0] = None
        df_temp = _get_hakai_flag_columns(df_temp, var, r"_qartod_flag")
        assert df_temp["salinity_flag_level_1"].iloc[0] == 4
        assert (df_temp["salinity_flag_level_1"].iloc[1:] == 1).all()
        assert df_temp["salinity_flag"].iloc[0].startswith("SVD")
        assert (df_temp["salinity_flag"].iloc[1:].str.startswith("AV")).all()
