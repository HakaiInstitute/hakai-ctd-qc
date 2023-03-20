import os

import numpy as np
import pandas as pd
import pytest

from hakai_profile_qc.__main__ import (
    _convert_time_to_datetime,
    _derived_ocean_variables,
    run_qc_profiles,
)

MODULE_PATH = os.path.dirname(__file__)
df_local = pd.read_parquet(f"{MODULE_PATH}/test_data/ctd_test_suite.parquet")
df_initial = df_local.set_index("ctd_data_pk").copy()
df_local = _derived_ocean_variables(df_local)
df_local = run_qc_profiles(df_local)
df_local = df_local.set_index("ctd_data_pk")


class TestDerivedVariables:
    def test_derive_variables_from_local(self):
        df_temp = _derived_ocean_variables(df_local)
        derived_variables = [
            "conservative_temperature",
            "sigma0",
            "absolute_salinity",
            "density",
        ]
        missing_derieved_variables = [
            var for var in derived_variables if var not in df_temp.columns
        ]
        assert (
            missing_derieved_variables != []
        ), f"missing derived variables {missing_derieved_variables}"


class TestTimeConversion:
    def test_full_dataframe_timeconversion(self):
        df_temp = _convert_time_to_datetime(df_local)
        assert isinstance(
            df_temp["start_dt"].dtype, object
        ), "Dataframe start_dt is not a datetime object"


class TestHakaiTests:
    def test_seabird_hakai_bad_value_test(self):
        df = df_initial.query("hakai_id == '01907674_2016-10-20T16:29:29Z'")
        assert (
            (df == -9.99e-29).any().any()
        ), "No seabird flag value -9.99E-29 is present in local test suite"

        df_flagged = df_local.loc[df.loc[(df == -9.99e-29).any(axis=1)].index]
        flagged_columns = [
            "descent_rate",
            "conductivity",
            "temperature",
            "depth",
            "par",
            "flc",
            "turbidity",
            "salinity",
            "dissolved_oxygen_ml_l",
            # "oxygen_voltage",
        ]
        assert (
            (
                df_flagged.filter(regex="|".join(flagged_columns)).filter(
                    regex="hakai_bad_value_test$"
                )
                == 4
            )
            .all()
            .all()
        ), "Not all the values -9.99E-29 were not flagged as FAIL=4"
        assert (
            (
                df_flagged.filter(regex="|".join(flagged_columns)).filter(
                    regex="_flag_level_1$"
                )
                == 4
            )
            .all()
            .all()
        ), "Not all the values -9.99E-29 were not flagged as *_flag_level_1=FAIL(=4)"
        assert (
            df_flagged.filter(regex="|".join(flagged_columns))
            .filter(regex="_flag$")
            .applymap(lambda x: str(x).startswith("SVD"))
            .all()
            .all()
        ), "Not all the values -9.99E-29 were not flagged as *_flag=SVD"
        assert (
            df_flagged.filter(regex="|".join(flagged_columns))
            .filter(regex="_flag$")
            .applymap(lambda x: "hakai_bad_value_test" in str(x))
            .all()
            .all()
        ), "Not all the values -9.99E-29 *_flag column contains the expression 'hakai_bad_value_test'"

    def test_missing_whole_profile_bad_value_test(self):
        df = df_local.query("hakai_id == '01907674_2018-10-31T19:10:18Z'")
        assert (
            not df.empty
        ), "Missing hakai_id=='01907674_2018-10-31T19:10:18Z' from local test suite"
        assert (
            df["dissolved_oxygen_ml_l_hakai_bad_value_test"] == 9
        ).all(), "Not all oxygen missing bad_value_test are flagged as MISSING=9"
        assert (
            df.loc[
                df["dissolved_oxygen_ml_l_hakai_bad_value_test"] == 9,
                "dissolved_oxygen_ml_l_flag_level_1",
            ]
            == 9
        ).all(), "Not all oxygen missing bad_value_test are flagged as MISSING=9"

    def test_missing_value_bad_value_test(self):
        for variable in df_local.columns:
            qartod_flag_variable = f"{variable}_hakai_bad_value_test"
            if qartod_flag_variable in df_local.columns:
                assert (
                    df_local.loc[df_local[variable].isna(), qartod_flag_variable]
                    .isin([4, 9])
                    .all(),
                    f"Not all '{variable}'.isna() is flagged as FAIL=4 or MISSING=9",
                )
                is_bad_value_flagged = df_local[qartod_flag_variable].isin([4, 9])
                assert (
                    df_local.loc[is_bad_value_flagged, qartod_flag_variable]
                    == df_local.loc[is_bad_value_flagged, f"{variable}_flag_level_1"],
                    "Bad value flag isn't matching flag_level_1",
                )


class TestQARTODTests:
    def test_gross_range_results(self):
        df = df_local.query("hakai_id == '01907674_2016-10-18T18:09:33Z'")
        assert (
            not df.empty
        ), "Missing test hakai_id=='01907674_2016-10-18T18:09:33Z' in local test suite"
        assert (
            len(df.loc[df["dissolved_oxygen_ml_l_qartod_gross_range_test"] == 4])
            == 249,
            "Missing qartod gross range result in dissolved_oxygen_ml_l_flag_level_1",
        )
        assert (
            df.loc[
                df["dissolved_oxygen_ml_l_qartod_gross_range_test"] == 4,
                "dissolved_oxygen_ml_l_flag_level_1",
            ]
            == 4
        ).all(), (
            "Missing qartod gross range result in dissolved_oxygen_ml_l_flag_level_1"
        )
        assert (
            (
                df.loc[
                    df["dissolved_oxygen_ml_l_qartod_gross_range_test"] == 4,
                    "dissolved_oxygen_ml_l_flag",
                ].str.contains("SVD: dissolved_oxygen_ml_l_qartod_gross_range_test")
            ).all(),
            "Missing qartod gross range result in dissolved_oxygen_ml_l_flag_level_1",
        )
