import pandas as pd
import pytest

from hakai_profile_qc.__main__ import _get_hakai_flag_columns

@pytest.fixture(scope="function")
def df():
    return pd.DataFrame(
        {
            "x": [1, 2, 3, 4, 5, 6],
            "x_qartod_flag_1": [1, 1, 1, 1, 1, 1],
            "x_qartod_flag_2": [1, 1, 1, 1, 1, 1],
        },
        index=[0, 1, 2, 3, 4, 5],
    )
var = "x"


class TestHakaiFlags:
    def test_hakai_flag_all_good(self, df):
        df_result = _get_hakai_flag_columns(df, var, r"_qartod_flag")
        assert (df_result["x_flag_level_1"] == 1).all()
        assert (df_result["x_flag"].str.startswith("AV")).all()

    def test_hakai_flag_all_good_with_unknown(self,df):
        df.loc[0,"x_qartod_flag_1"] = 2
        df = _get_hakai_flag_columns(df, var, r"_qartod_flag")
        assert (df["x_flag_level_1"] == 1).all()
        assert (df["x_flag"].str.startswith("AV")).all()

    def test_hakai_flag_all_good_with_suspect(self,df):
        df.loc[0,"x_qartod_flag_1"] = 3
        df = _get_hakai_flag_columns(df, var, r"_qartod_flag")
        assert df.loc[0,"x_flag_level_1"] == 3
        assert df.loc[0,"x_flag"].startswith("SVC")
        assert not df.loc[df.index[1:],"x_flag"].str.contains("SVC: ").any()
        assert (df.loc[df.index[1:],"x_flag"].str.startswith("AV")).all()

    def test_hakai_flag_all_good_with_fail(self,df):
        df.loc[0,"x_qartod_flag_1"] = 4
        df = _get_hakai_flag_columns(df, var, r"_qartod_flag")
        assert df.loc[0,"x_flag_level_1"] == 4
        assert df["x_flag"].iloc[0].startswith("SVD")
        assert "SVC: " not in df["x_flag"]
        assert (df.loc[df.index[1:],"x_flag"].str.startswith("AV")).all()

    def test_hakai_flag_all_good_with_fail_and_suspect(self,df):
        df.loc[0,"x_qartod_flag_1"] = 4
        df.loc[0,"x_qartod_flag_2"] = 3
        df = _get_hakai_flag_columns(df, var, r"_qartod_flag")
        assert df.loc[0,"x_flag_level_1"] == 4
        assert (df.loc[df.index[1:],"x_flag_level_1"] == 1).all()
        assert df["x_flag"].iloc[0].startswith("SVD")
        assert "SVC: " not in df["x_flag"]
        assert (df.loc[df.index[1:],"x_flag"].str.startswith("AV")).all()

    def test_hakai_flag_all_good_with_suspect_and_fail(self,df):
        df.loc[0,"x_qartod_flag_1"] = 3
        df.loc[0,"x_qartod_flag_2"] = 4
        df = _get_hakai_flag_columns(df, var, r"_qartod_flag")
        assert df.loc[0,"x_flag_level_1"] == 4
        assert (df.loc[df.index[1:],"x_flag_level_1"] == 1).all()
        assert df["x_flag"].iloc[0].startswith("SVD")
        assert "SVC: " not in df["x_flag"]
        assert (df.loc[df.index[1:],"x_flag"].str.startswith("AV")).all()

    def test_hakai_flag_all_good_with_fail_and_suspect_ignore_other_flag(self,df):
        df.loc[0,"x_qartod_flag_1"] = 4
        df.loc[0,"x_qartod_flag_2"] = 3
        df = _get_hakai_flag_columns(df, var, r"_qartod_flag_1")
        assert df.loc[0,"x_flag_level_1"] == 4
        assert (df.loc[df.index[1:],"x_flag_level_1"] == 1).all()
        assert df["x_flag"].iloc[0].startswith("SVD")
        assert "SVC: " not in df.loc[0,"x_flag"]
        assert (df.loc[df.index[1:],"x_flag"].str.startswith("AV")).all()

    def test_hakai_flag_all_good_with_fail_and_null_value(self,df):
        df.loc[0,"x_qartod_flag_1"] = 4
        df.loc[0,"x"] = None
        df = _get_hakai_flag_columns(df, var, r"_qartod_flag")
        assert df.loc[0,"x_flag_level_1"] == 4
        assert (df.loc[df.index[1:],"x_flag_level_1"] == 1).all()
        assert df.loc[0,"x_flag"].startswith("SVD")
        assert (df.loc[df.index[1:],"x_flag"].str.startswith("AV")).all()
