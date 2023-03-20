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
