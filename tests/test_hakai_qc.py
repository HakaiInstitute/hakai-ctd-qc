import pandas as pd
import pytest

from hakai_ctd_qc.__main__ import _get_hakai_flag_columns


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

    def test_hakai_flag_all_good_with_unknown(self, df):
        df.loc[0, "x_qartod_flag_1"] = 2
        df = _get_hakai_flag_columns(df, var, r"_qartod_flag")
        assert (df["x_flag_level_1"] == 1).all()
        assert (df["x_flag"].str.startswith("AV")).all()

    def test_hakai_flag_all_good_with_suspect(self, df):
        df.loc[0, "x_qartod_flag_1"] = 3
        df = _get_hakai_flag_columns(df, var, r"_qartod_flag")
        assert df.loc[0, "x_flag_level_1"] == 3
        assert df.loc[0, "x_flag"].startswith("SVC")
        assert not df.loc[df.index[1:], "x_flag"].str.contains("SVC: ").any()
        assert (df.loc[df.index[1:], "x_flag"].str.startswith("AV")).all()

    def test_hakai_flag_all_good_with_fail(self, df):
        df.loc[0, "x_qartod_flag_1"] = 4
        df = _get_hakai_flag_columns(df, var, r"_qartod_flag")
        assert df.loc[0, "x_flag_level_1"] == 4
        assert df["x_flag"].iloc[0].startswith("SVD")
        assert "SVC: " not in df["x_flag"]
        assert (df.loc[df.index[1:], "x_flag"].str.startswith("AV")).all()

    def test_hakai_flag_all_good_with_fail_and_suspect(self, df):
        df.loc[0, "x_qartod_flag_1"] = 4
        df.loc[0, "x_qartod_flag_2"] = 3
        df = _get_hakai_flag_columns(df, var, r"_qartod_flag")
        assert df.loc[0, "x_flag_level_1"] == 4
        assert (df.loc[df.index[1:], "x_flag_level_1"] == 1).all()
        assert df["x_flag"].iloc[0].startswith("SVD")
        assert "SVC: " not in df["x_flag"]
        assert (df.loc[df.index[1:], "x_flag"].str.startswith("AV")).all()

    def test_hakai_flag_all_good_with_suspect_and_fail(self, df):
        df.loc[0, "x_qartod_flag_1"] = 3
        df.loc[0, "x_qartod_flag_2"] = 4
        df = _get_hakai_flag_columns(df, var, r"_qartod_flag")
        assert df.loc[0, "x_flag_level_1"] == 4
        assert (df.loc[df.index[1:], "x_flag_level_1"] == 1).all()
        assert df["x_flag"].iloc[0].startswith("SVD")
        assert "SVC: " not in df["x_flag"]
        assert (df.loc[df.index[1:], "x_flag"].str.startswith("AV")).all()

    def test_hakai_flag_all_good_with_fail_and_suspect_ignore_other_flag(self, df):
        df.loc[0, "x_qartod_flag_1"] = 4
        df.loc[0, "x_qartod_flag_2"] = 3
        df = _get_hakai_flag_columns(df, var, r"_qartod_flag_1")
        assert df.loc[0, "x_flag_level_1"] == 4
        assert (df.loc[df.index[1:], "x_flag_level_1"] == 1).all()
        assert df["x_flag"].iloc[0].startswith("SVD")
        assert "SVC: " not in df.loc[0, "x_flag"]
        assert (df.loc[df.index[1:], "x_flag"].str.startswith("AV")).all()

    def test_hakai_flag_with_null_value(self, df):
        df.loc[0, "x"] = None
        df = _get_hakai_flag_columns(df, var, r"_qartod_flag")
        assert (
            df.loc[0][["x", "x_flag", "x_flag_level_1"]].isna().all()
        ), "All value, flag and flag_level_1 should be NaN"
        assert (df.loc[df.index[1:], "x_flag_level_1"] == 1).all()
        assert (df.loc[df.index[1:], "x_flag"].str.startswith("AV")).all()

    def test_hakai_flag_null(self, df):
        df.loc[0, "x_qartod_flag_1"] = None
        df = _get_hakai_flag_columns(df, var, r"_qartod_flag")
        assert pd.isna(df.loc[0, "x_flag"]), "Flag should be null"
        assert df.loc[0, "x_flag_level_1"] == 1
        assert (df.loc[df.index[1:], "x_flag"].str.startswith("AV")).all()
        assert (df.loc[df.index[1:], "x_flag_level_1"] == 1).all()

    def test_hakai_flag_all_null(self, df):
        df.loc[0, "x_qartod_flag_1"] = None
        df.loc[0, "x_qartod_flag_2"] = None
        df = _get_hakai_flag_columns(df, var, r"_qartod_flag")
        assert pd.isna(df.loc[0, "x_flag"]), "Flag should be null"
        assert pd.isna(df.loc[0, "x_flag_level_1"]), "Flag level 1 should be null"
        assert (df.loc[df.index[1:], "x_flag"].str.startswith("AV")).all()
        assert (df.loc[df.index[1:], "x_flag_level_1"] == 1).all()

    def test_hakai_flag_all_null_with_unknown(self, df):
        df.loc[0, "x_qartod_flag_1"] = None
        df.loc[0, "x_qartod_flag_2"] = 2
        df = _get_hakai_flag_columns(df, var, r"_qartod_flag")
        assert pd.isna(df.loc[0, "x_flag"]), "Flag should be null"
        assert pd.isna(df.loc[0, "x_flag_level_1"]), "Flag level 1 should be null"
        assert (df.loc[df.index[1:], "x_flag"].str.startswith("AV")).all()
        assert (df.loc[df.index[1:], "x_flag_level_1"] == 1).all()
