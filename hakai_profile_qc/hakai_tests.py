"""Hakai Tests
Regroup Hakai CTD profiles specific tests to be applied during the QC step.
"""

import re
import warnings

import numpy as np
import pandas as pd
from ioos_qc.qartod import QartodFlags
from loguru import logger

# Import Hakai Station List

qartod_to_hakai_flag = {1: "AV", 2: "NA", 3: "SVC", 4: "SVD", 9: "MV"}


def do_cap_test(
    df,
    var,
    profile_id="hakai_id",
    direction_flag="direction_flag",
    depth_var="depth",
    bin_size=1,
    suspect_threshold=0.2,
    fail_threshold=0.5,
    ratio_above_threshold=0.5,
    minimum_bins_per_profile=10,
    flag_name="_do_cap_test",
):
    """
    Hakai do_cap_test compare down and up cast values measured by an instrument at the same depth. The test compare
    the number of records that has a different value above the suspect or fail threshold versus the total number of
    similar value available for each cast. If n_suspect/n_values is above suspect/fail threshold, the whole profile
    is flagged accordingly.

    INPUTS:
    df: dataframe
    var: variable to review up/down cast values
    depth_var: variable describing the vertical coordinate
    direction_flag: variable describing the direction of the profile
    bin_size: vertical bin size to apply the test to
    suspect_threshold: suspect threshold value for detection |X_nu - X_nd|
    fail_threshold: suspect threshold value for detection |X_nu - X_nd|
    ratio_above_threshold: minimum threshold of fraction of suspect/fail binned value to consider to flag profile
    minimum_bins_per_profile: minimum amount of bins necessary to make the test usable.

    ASSUMPTIONS:
    As of now, the test assume that the input data is already bin averaged for both up or down cast.

    OUTPUT:
    The test will generate an extra column [var]_do_cap_test with QARTOD flag.
    """

    def _get_do_cap_flag(result):
        if result["is_missing"] == 1:
            return QartodFlags.MISSING
        elif (
            result["nGoodBinsPerProfile"] < minimum_bins_per_profile
            or result["is_missing"] + result["is_unknown"] == 1
        ):
            return QartodFlags.UNKNOWN
        elif result["is_fail"] > ratio_above_threshold:
            return QartodFlags.FAIL
        elif result["is_suspect"] > ratio_above_threshold:
            return QartodFlags.SUSPECT
        return QartodFlags.GOOD

    # Handle empty inputs or with no upcast data.
    if var not in df or df[var].isna().all():
        df[var + flag_name] = QartodFlags.MISSING
        return df

    # Bin average average record associated to each profile,direction and bin_id
    # and then calculate the difference between the two direction
    profile_bin_stats = (
        df.assign(bin_id=(df[depth_var] / bin_size).round())
        .groupby([profile_id, direction_flag, "bin_id"])[var]
        .mean(numeric_only=True)
        .groupby([profile_id, "bin_id"])
        .agg([np.ptp, "count"])
    )

    # Define missing, unknown, suspect and missing values from bin statistics
    profile_bin_stats = profile_bin_stats.assign(
        is_missing=profile_bin_stats["ptp"].isnull()
        & (profile_bin_stats["count"] == 0),
        is_unknown=profile_bin_stats["count"] == 1,
        is_suspect=(profile_bin_stats["ptp"] > suspect_threshold)
        & (profile_bin_stats["count"] > 1),
        is_fail=(profile_bin_stats["ptp"] > fail_threshold)
        & (profile_bin_stats["count"] > 1),
    )

    # Get each flag ration per profile
    profile_stats = profile_bin_stats.groupby(by=[profile_id]).agg(
        lambda x: sum(x) / len(x)
    )
    profile_stats["nBinsPerProfile"] = profile_bin_stats.groupby(by=[profile_id])[
        "count"
    ].count()
    profile_stats["nGoodBinsPerProfile"] = (
        profile_bin_stats.query("ptp>0")["ptp"].groupby("hakai_id").count()
    )

    # Generate quartod flag
    profile_stats[var + flag_name] = profile_stats.apply(
        _get_do_cap_flag, axis="columns"
    )

    return df.merge(profile_stats[var + flag_name], how="left", on="hakai_id")


def bottom_hit_detection(
    df,
    variables,
    profile_id="hakai_id",
    depth_variable="depth",
    profile_direction_variable="direction_flag",
    flag_column_name="bottom_hit_test",
):
    """
    Method that flag consecutive data near the bottom of a profile that was flagged SUSPECT=3 or FAIl=4. Output a
    'bottom_hit_flag' channel.
    """

    # For each profile (down and up cast), get the density flag value for the deepest record.
    #  If flagged [3,4], it has likely hit the bottom.
    df[flag_column_name] = QartodFlags.GOOD

    bottom_hit_id = (
        df.sort_values(by=[profile_id, profile_direction_variable, depth_variable])
        .groupby(by=[profile_id, profile_direction_variable])
        .last()[variables]
        .isin([QartodFlags.SUSPECT, QartodFlags.FAIL])
    )

    # Now let's flag the consecutive data that are flagged in sigma0 near the bottom as bottom hit
    for hakai_id in bottom_hit_id[bottom_hit_id].reset_index()[profile_id]:
        for _, df_bottom_hit in df[df[profile_id] == hakai_id].groupby(
            by=[profile_id, profile_direction_variable]
        ):
            # For each bottom hit find the deepest good record in density and flag everything else below as FAIL
            df.loc[
                df_bottom_hit[
                    df_bottom_hit[depth_variable]
                    > df_bottom_hit[df_bottom_hit[variables] == 1][depth_variable].max()
                ].index,
                flag_column_name,
            ] = QartodFlags.FAIL
    return df


def par_shadow_test(
    df,
    variable="par",
    min_par_for_shadow_detection=5,
    profile_id="hakai_id",
    direction_flag="direction_flag",
    depth_var="depth",
    flag_column_name="par_shadow_test",
):
    """
    The PAR shadow test assume that PAR values should always be increasing with shallower depths. The tool first
    sort the data along the pressure for each individual profiles and compute the cumulative maximum value recorded
    from the bottom to the surface. A PAR value is flagged as SUSPECT if the value is bigger than the
    min_par_for_shadow_detection provided value and lower than the cumulative maximum value.
    """
    # Detect PAR Shadow
    if df[variable].isna().all():
        df[flag_column_name] = QartodFlags.UNKNOWN
    else:
        df["par_cummax"] = (
            df.sort_values(by=[profile_id, direction_flag, depth_var], ascending=False)
            .groupby(by=[profile_id, direction_flag])[variable]
            .cummax()
        )

        df[flag_column_name] = QartodFlags.GOOD
        df.loc[
            (df[variable] < df["par_cummax"])
            & (df["par_cummax"] > min_par_for_shadow_detection),
            flag_column_name,
        ] = QartodFlags.SUSPECT
        df.drop("par_cummax", axis=1, inplace=True)
    return df


def bad_value_test(
    df, variables, flag_mapping=None, flag_column_suffix="_hakai_bad_value_test"
):
    """
    Find Flag values present in the data, attach a given QARTOD Flag to them and replace them by NaN.
    """
    # Default Hakai Bad data
    if flag_mapping is None:
        flag_mapping = {"MISSING": [np.nan, pd.NA], "FAIL": [-9.99e-29]}

    for column in variables:
        # Assign everything as good first
        logger.debug("Generate flag column: {}{}", column, flag_column_suffix)
        df[column + flag_column_suffix] = QartodFlags.GOOD
        for level, values in flag_mapping.items():
            is_na_values = [
                value
                for value in values
                if pd.isna(value) or value in [".isna", "NaN", "nan"]
            ]
            if any(is_na_values):
                df.loc[df[column].isna(), column + flag_column_suffix] = (
                    QartodFlags.__dict__[level]
                )
                values = set(values).difference(is_na_values)
            df.loc[df[column].isin(values), column + flag_column_suffix] = (
                QartodFlags.__dict__[level]
            )
    return df


def load_grey_list(path):
    return pd.read_csv(
        path,
        dtype={
            "device_model": str,
            "device_sn": str,
            "hakai_id": str,
            "query": str,
            "data_type": str,
            "flag_type": int,
        },
        parse_dates=["start_datetime_range", "end_datetime_range"],
    ).replace({pd.NA: None})


def grey_list(
    df,
    df_grey_list,
    level1_flag_suffix="_flag_level_1",
    level2_flag_suffix="_flag",
    grey_list_suffix="_grey_list_test",
):
    # Loop through each lines
    # Since the grey list is a manual input it will likely be small amount and looping through
    # each should be good enough for now. We may have to filter the grey list based on the input in the future
    # if the grey list becomes significant.
    for _, row in df_grey_list.iterrows():
        # Generate Grey List Entry Query
        # Mandatory fields
        query_string = f"'{row['start_datetime_range']}' <= measurement_dt <= '{row['end_datetime_range']}'"
        query_string += f" and device_model=='{row['device_model']}'"
        query_string += f" and device_sn=='{row['device_sn']}'"
        # Optional Fields
        if row["hakai_id"]:
            query_string += f" and hakai_id in ({row['hakai_id'].split(',')})"
        if row["query"]:
            query_string += row["query"]

        # Find matching data
        df_to_flag = df.query(query_string)

        # If some data needs to be flagged
        if len(df_to_flag) > 0:
            # Review if the columns exist
            unknown_variables = [
                var for var in row["data_type"].split(",") if var not in df.columns
            ]
            variable_list = [
                var for var in row["data_type"].split(",") if var in df.columns
            ]

            # Give warning if variable unavailable
            if unknown_variables:
                warnings.warn(
                    f"{unknown_variables} are not available and will be ignored",
                    category=RuntimeWarning,
                )

            # Retrieve flag columns
            grey_list_test_columns = [var + grey_list_suffix for var in variable_list]
            qartod_columns = [var + level1_flag_suffix for var in variable_list]
            flag_descriptor_columns = [
                var + level2_flag_suffix for var in variable_list
            ]

            # Add a grey list test column is missing
            missing_grey_list_flag = set(grey_list_test_columns) - set(df.columns)
            if missing_grey_list_flag:
                for var in missing_grey_list_flag:
                    df[var] = QartodFlags.GOOD

            # Add a grey list test variable for helping review
            df.loc[df_to_flag.index, grey_list_test_columns] = row["flag_type"]

            # Overwrite Hakai QARTOD Flag
            df.loc[df_to_flag.index, qartod_columns] = row["flag_type"]

            # Append to description Flag Comment and name
            grey_flag_comment = row.comments + (
                " flagged by " + row.flagged_by if row.flagged_by else ""
            )
            grey_flag_description = f"{qartod_to_hakai_flag[row['flag_type']]}: Hakai Grey List - {grey_flag_comment}"
            for column in flag_descriptor_columns:
                if column not in df:
                    df[column] = ""
                df.loc[df_to_flag.index, column] = df.loc[
                    df_to_flag.index, column
                ].apply(
                    lambda x: (
                        x + "; " + grey_flag_description if x else grey_flag_description
                    )
                )
    return df


def hakai_station_maximum_depth_test(
    df,
    hakai_stations,
    variable="depth",
    flag_column="depth_in_station_range_test",
    suspect_exceedance_percentage=None,
    fail_exceedance_percentage=None,
    suspect_exceedance_range=None,
    fail_exceedance_range=None,
):
    """
    This test review each profile maximum depth by profile identifier
    and compare it to the station depth. The whole profile
    gets flagged as suspect/fail if the maximum depth exceed a percentage
    or range above of the station depth.
    """
    # Get Maximum Depth per profile
    df_max_depth = (
        df.groupby(["station", "hakai_id"])[variable]
        .max()
        .rename("max_depth")
        .to_frame()
    )

    # Join Maximum Depth With station information
    df_max_depth = df_max_depth.reset_index().merge(
        hakai_stations[["station", "station_depth"]],
        how="left",
        on="station",
    )

    # Start with Flag GOOD
    df_max_depth[flag_column] = QartodFlags.GOOD

    # If station is depth is unknown flag unkown
    df_max_depth.loc[df_max_depth["station_depth"].isnull(), flag_column] = (
        QartodFlags.UNKNOWN
    )

    # SUSPECT Flag
    df_max_depth.loc[
        (
            df_max_depth["max_depth"]
            > df_max_depth["station_depth"] * suspect_exceedance_percentage
        )
        & (
            df_max_depth["max_depth"]
            > df_max_depth["station_depth"] + suspect_exceedance_range
        ),
        flag_column,
    ] = QartodFlags.SUSPECT

    # Fail Flag
    df_max_depth.loc[
        (
            df_max_depth["max_depth"]
            > df_max_depth["station_depth"] * fail_exceedance_percentage
        )
        & (
            df_max_depth["max_depth"]
            > df_max_depth["station_depth"] + fail_exceedance_range
        ),
        flag_column,
    ] = QartodFlags.FAIL

    return df.merge(
        df_max_depth.reset_index()[["station", "hakai_id", flag_column]],
        on=["station", "hakai_id"],
    )


def query_based_flag_test(df: pd.DataFrame, query_list: list):
    """
    Run each respective queries and apply associated flag to each respective matching results
    Arguments:
        df: dataframe of the data
        query_list: list of query objects ->
            {
                "query": run by pandas query,
                "flag_value": QARTOD value,
                "flag_columns": list of column names to generate
            }
    Output:  dataframe
    """

    for query in query_list:
        for flag_column in query["flag_columns"]:
            if flag_column not in df:
                df[flag_column] = 1  # GOOD

        df.loc[df.query(query["query"]).index, query["flag_columns"]] = query[
            "flag_value"
        ]

    return df


def apply_flag_from_process_log(df, metadata):
    """
    Apply flag from processing log to the dataframe respective variables
    """
    for _, cast in metadata.iterrows():
        if not cast["process_log"] or not re.search(
            "warning", cast["process_log"], re.IGNORECASE
        ):
            continue

        if (
            "WARNING!!! Slower Oxygen Sensor RBR CODAstandard are not recommended for profiling applications."
            in cast["process_log"]
            and cast["cast_type"] != "Static"
        ):
            df.loc[
                df["hakai_id"] == cast["hakai_id"],
                "dissolved_oxygen_ml_l_hakai_slow_oxygen_sensor_test",
            ] = 3
            df.loc[
                df["hakai_id"] == cast["hakai_id"],
                "dissolved_oxygen_ml_l_hakai_slow_oxygen_sensor_test",
            ] = 3

        if "WARNING! NO SOAK DETECTED, SUSPICIOUS DATA QUALITY" in cast["process_log"]:
            df.loc[
                df["hakai_id"] == cast["hakai_id"],
                "dissolved_oxygen_ml_l_hakai_no_soak_test",
            ] = 3
            df.loc[
                df["hakai_id"] == cast["hakai_id"], "temperature_hakai_no_soak_test"
            ] = 3
            df.loc[
                df["hakai_id"] == cast["hakai_id"], "conductivity_hakai_no_soak_test"
            ] = 3
            df.loc[
                df["hakai_id"] == cast["hakai_id"], "salinity_hakai_no_soak_test"
            ] = 3

        if "Static Measurement is considered SUSPICIOUS due to the lowered thredholds" in cast["process_log"]:
            df.loc[
                df["hakai_id"] == cast["hakai_id"], "hakai_short_static_deployment_test"
            ] = 3

    return df
