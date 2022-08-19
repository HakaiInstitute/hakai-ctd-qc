import argparse
import json
import logging
import os

import gsw
import numpy as np
import pandas as pd
from hakai_api import Client
from ioos_qc.config import Config
from ioos_qc.qartod import QartodFlags, qartod_compare
from ioos_qc.stores import PandasStore
from ioos_qc.streams import PandasStream
from tqdm import tqdm

import hakai_tests

logger = logging.getLogger(__name__)

tqdm.pandas()
config_path = os.path.join(os.path.dirname(__file__), "config")
ioos_to_hakai_coors = dict(
    tinp="measurement_dt", zinp="depth", lon="longitude", lat="latitude"
)
CTD_CAST_ENDPOINT = "/ctd/views/file/cast"
CTD_CAST_DATA_ENDPOINT = "/ctd/views/file/cast/data"
subset_ctd_variables = [
    "ctd_file_pk",
    "ctd_cast_pk",
    "hakai_id",
    "ctd_data_pk",
    "filename",
    "device_model",
    "device_sn",
    "device_firmware",
    "file_processing_stage",
    "work_area",
    "cruise",
    "station",
    "cast_number",
    "station_longitude",
    "station_latitude",
    "distance_from_station",
    "latitude",
    "longitude",
    "location_flag",
    "location_flag_level_1",
    "process_flag",
    "process_flag_level_1",
    "start_dt",
    "bottom_dt",
    "end_dt",
    "duration",
    "start_depth",
    "bottom_depth",
    "target_depth",
    "drop_speed",
    "vessel",
    "direction_flag",
    "measurement_dt",
    "descent_rate",
    "conductivity",
    "conductivity_flag",
    "conductivity_flag_level_1",
    "temperature",
    "temperature_flag",
    "temperature_flag_level_1",
    "depth",
    "depth_flag",
    "depth_flag_level_1",
    "pressure",
    "pressure_flag",
    "pressure_flag_level_1",
    "par",
    "par_flag",
    "par_flag_level_1",
    "flc",
    "flc_flag",
    "flc_flag_level_1",
    "turbidity",
    "turbidity_flag",
    "turbidity_flag_level_1",
    "ph",
    "ph_flag",
    "ph_flag_level_1",
    "salinity",
    "salinity_flag",
    "salinity_flag_level_1",
    "spec_cond",
    "spec_cond_flag",
    "spec_cond_flag_level_1",
    "dissolved_oxygen_ml_l",
    "dissolved_oxygen_ml_l_flag",
    "dissolved_oxygen_ml_l_flag_level_1",
    "rinko_do_ml_l",
    "rinko_do_ml_l_flag",
    "rinko_do_ml_l_flag_level_1",
    "dissolved_oxygen_percent",
    "dissolved_oxygen_percent_flag",
    "dissolved_oxygen_percent_flag_level_1",
    "oxygen_voltage",
    "oxygen_voltage_flag",
    "oxygen_voltage_flag_level_1",
    "c_star_at",
    "c_star_at_flag",
    "c_star_at_flag_level_1",
    "sos_un",
    "sos_un_flag",
    "sos_un_flag_level_1",
    "backscatter_beta",
    "backscatter_beta_flag",
    "backscatter_beta_flag_level_1",
    "cdom_ppb",
    "cdom_ppb_flag",
    "cdom_ppb_flag_level_1",
]


def run_ioosqc_on_dataframe(df, qc_config, tinp="t", zinp="z", lat="lat", lon="lon"):
    """
    Apply ioos_qc configuration to a Pandas DataFrame by using the PandasStream mand PandasStore methods from ioos_qc
    """
    # Reset index ioos_qc failed otherwise
    original_index = list(df.index.names)
    if original_index == [None]:
        original_index = ["index"]
    df = df.reset_index()

    # Set Stream
    stream = PandasStream(df, time=tinp, z=zinp, lat=lat, lon=lon)
    # Set Configuration
    c = Config(qc_config)
    # Run
    results = stream.run(c)
    # Save to a pandas DataFrame
    store = PandasStore(results, axes={"t": tinp, "z": zinp, "lat": lat, "lon": lon})
    result_store = store.save(write_data=False, write_axes=False)

    return df.join(result_store).set_index(original_index)


def generate_process_flags_json(cast, data):
    """
    Generate a JSON representation of the qced data compatible the
    hakai_api endpoint "{api_root}/ctd/process/flags/json/{row['ctd_cast_pk']}"
    """
    return json.dumps(
        {
            "cast": json.loads(
                cast[
                    ["ctd_cast_pk", "hakai_id", "processing_stage", "process_error"]
                ].to_json()
            ),
            "ctd_data": json.loads(
                data.query(f"hakai_id=='{cast['hakai_id']}'")
                .filter(regex="^ctd_data_pk$|_flag$|_flag_level_1$")
                .drop(
                    columns=[
                        "direction_flag",
                        "process_flag",
                        "process_flag_level_1",
                        "location_flag",
                        "location_flag_level_1",
                    ]
                )
                .to_json(orient="records")
            ),
        }
    )


def derived_ocean_variables(df):
    """Compute Derived Variables with TEOS-10 equations"""
    longitude = df["station_longitude"].fillna(df["longitude"])
    latitude = df["station_latitude"].fillna(df["latitude"])
    df["absolute salinity"] = gsw.SA_from_SP(
        df["salinity"], df["pressure"], longitude, latitude
    )
    df["conservative temperature"] = gsw.CT_from_t(
        df["absolute salinity"], df["temperature"], df["pressure"]
    )
    df["density"] = gsw.rho(
        df["absolute salinity"], df["conservative temperature"], df["pressure"]
    )
    df["sigma0"] = gsw.sigma0(df["absolute salinity"], df["conservative temperature"])
    return df


def run_qc_profiles(
    df,
    qartod_config,
    hakai_tests_config=None,
):
    """
    Main method that runs on a number of profiles a series of QARTOD tests and specific to the Hakai CTD Dataset.
    """
    # Read configurations
    # QARTOD
    if qartod_config is None:
        qartod_config = os.path.join(
            config_path, "hakai_ctd_profile_qartod_test_config.json"
        )
    if isinstance(qartod_config, str):
        logger.info("Load Default QARTOD Configuration: %s", qartod_config)
        with open(qartod_config) as f:
            qartod_config = json.loads(f.read())
    # HAKAI TESTS
    if hakai_tests_config is None:
        hakai_tests_config = os.path.join(
            config_path, "hakai_ctd_profile_tests_config.json"
        )
    if isinstance(hakai_tests_config, str):
        logger.info("Load Default Hakai Tests Configuration: %s", hakai_tests_config)
        with open(hakai_tests_config) as f:
            hakai_tests_config = json.loads(f.read())

    # Regroup profiles by profile_id and direction and sort them along zinp
    df = df.sort_values(by=["hakai_id", "direction_flag", "depth"])

    # Retrieve tested variables list
    tested_variable_list = []
    for context in qartod_config["contexts"]:
        for stream, _ in context["streams"].items():
            if stream not in tested_variable_list:
                tested_variable_list += [stream]

    # Find Flag values present in the data, attach a FAIL QARTOD Flag to them and replace them by NaN.
    #  Hakai database ingested some seabird flags -9.99E-29 which need to be recognized and removed.
    if "bad_value_test" in hakai_tests_config:
        logger.info(
            "Flag Bad Values: %s",
            str(hakai_tests_config["bad_value_test"]["flag_list"]),
        )
        df = hakai_tests.bad_value_test(
            df,
            variables=hakai_tests_config["bad_value_test"]["variables"],
            flag_list=hakai_tests_config["bad_value_test"]["flag_list"],
        )

    # Run QARTOD tests
    # On profiles
    tqdm.pandas(desc="Apply QARTOD Tests to individual profiles", unit=" profile")
    df_profiles = (
        df.query("direction_flag in ('d','u')")
        .groupby(["hakai_id", "direction_flag"], as_index=False)
        .progress_apply(
            lambda x: run_ioosqc_on_dataframe(x, qartod_config, **ioos_to_hakai_coors),
        )
    )
    # On static measurements
    tqdm.pandas(
        desc="Apply QARTOD Tests to individual static measurements",
        unit=" measurement",
    )
    df_static = (
        df.query("direction_flag in ('s')")
        .groupby(["hakai_id", "measurement_dt"])
        .progress_apply(
            lambda x: run_ioosqc_on_dataframe(x, qartod_config, **ioos_to_hakai_coors),
        )
    )
    # Regroup back together profiles and static data
    df = df_profiles.append(df_static).reset_index(drop=True)

    # HAKAI SPECIFIC TESTS #
    # This section regroup different non QARTOD tests which are specific to Hakai profile dataset. Most of the them
    # uses the pandas dataframe to transform the data and apply divers tests.
    # DO CAP DETECTION
    if any(df["direction_flag"] == "u") and ("do_cap_test" in hakai_tests_config):
        for key in hakai_tests_config["do_cap_test"]["variable"]:
            logger.info("DO Cap Detection to %s variable", key)
            hakai_tests.do_cap_test(
                df,
                key,
                profile_id="hakai_id",
                depth_var="depth",
                direction_flag="direction_flag",
                bin_size=hakai_tests_config["do_cap_test"]["bin_size"],
                suspect_threshold=hakai_tests_config["do_cap_test"][
                    "suspect_threshold"
                ],
                fail_threshold=hakai_tests_config["do_cap_test"]["fail_threshold"],
                ratio_above_threshold=hakai_tests_config["do_cap_test"][
                    "ratio_above_threshold"
                ],
                minimum_bins_per_profile=hakai_tests_config["do_cap_test"][
                    "minimum_bins_per_profile"
                ],
            )

    # BOTTOM HIT DETECTION
    #  Find Profiles that were flagged near the bottom and assume this is likely related to having it the bottom.
    if "bottom_hit_detection" in hakai_tests_config:
        logger.info("Flag Bottom Hit Data")
        df = hakai_tests.bottom_hit_detection(
            df,
            variables=hakai_tests_config["bottom_hit_detection"]["variable"],
            profile_id="hakai_id",
            depth_variable="depth",
            profile_direction_variable="direction_flag",
        )

    # Detect PAR Shadow
    if "par_shadow_test" in hakai_tests_config:
        logger.info("Flag PAR Shadow Data")
        df = hakai_tests.par_shadow_test(
            df,
            variable=hakai_tests_config["par_shadow_test"]["variable"],
            min_par_for_shadow_detection=hakai_tests_config["par_shadow_test"][
                "min_par_for_shadow_detection"
            ],
            profile_id="hakai_id",
            direction_flag="direction_flag",
            depth_var="depth",
        )

    if "depth_range_test" in hakai_tests_config:
        logger.info("Review maximum depth per profile vs station")
        df = hakai_tests.hakai_station_maximum_depth_test(
            df,
            variable=hakai_tests_config["depth_range_test"]["variables"],
            suspect_exceedance_percentage=hakai_tests_config["depth_range_test"][
                "suspect_exceedance_percentage"
            ],
            fail_exceedance_percentage=hakai_tests_config["depth_range_test"][
                "fail_exceedance_percentage"
            ],
            suspect_exceedance_range=hakai_tests_config["depth_range_test"][
                "suspect_exceedance_range"
            ],
            fail_exceedance_range=hakai_tests_config["depth_range_test"][
                "fail_exceedance_range"
            ],
        )

    # Add Station Maximum Depth Test

    # APPLY QARTOD FLAGS FROM ONE CHANNEL TO OTHER AGGREGATED ONES
    # Generate Hakai Flags
    for var in tested_variable_list:
        logger.info("Apply flag results to %s", var)

        # Extra flags that apply to all variables
        extra_flags = (
            "|bottom_hit_test|depth_in_station_range_test"
            + "|pressure_qartod_gross_range_test|depth_qartod_gross_range_test"
        )

        # Add Density Inversion to selected variables
        if var in ["temperature", "salinity", "conductivity"]:
            extra_flags = extra_flags + "|sigma0_qartod_density_inversion_test"

        # Add DO Cap Flag
        if var in ["dissolved_oxygen_ml_l", "rinko_ml_l"]:
            extra_flags = extra_flags + "|" + var + "_do_cap_test"

        # Create Hakai Flag Columns
        df = get_hakai_flag_columns(df, var, extra_flags)

    # Apply Hakai Grey List
    # Grey List should overwrite the QARTOD Flags
    logger.info("Apply Hakai Grey List")
    df = hakai_tests.grey_list(df)

    return df


def qc_hakai_profiles(
    processed_cast_filter=None,
    server="goose",
    chunksize=100,
    update_server=False,
    qartod_config=None,
    hakai_tests_config=None,
):

    if processed_cast_filter is None:
        logger.info("QC processed data that is not qced yet")
        # QC data that has been processed but not qced yet
        processed_cast_filter = "processing_stage={8_rbr_processed,8_binAvg}&limit=-1"

    # Connect to the server
    client = Client()
    if server == "hecate":
        api_root = client.api_root
    elif server == "goose":
        api_root = "https://goose.hakai.org/api"
    else:
        raise RuntimeError("Unknown server! goose/hecate")

    # Get the list of hakai_ids to qc
    url = f"{api_root}{CTD_CAST_ENDPOINT}?{processed_cast_filter}"
    response = client.get(url)
    df_casts = pd.DataFrame(response.json())
    if df_casts.empty:
        logger.info("No Drops needs to be QC")
        return None, None
    logger.info("%s needs to be qc!", len(df_casts))

    qced_cast_data = []
    for chunk in np.array_split(df_casts, np.ceil(len(df_casts) / chunksize)):
        logger.debug("QC hakai_ids: %s", str(chunk["hakai_id"]))
        response_data = client.get(
            f"{api_root}/{CTD_CAST_DATA_ENDPOINT}?hakai_id="
            + "{"
            + ",".join(chunk["hakai_id"].values)
            + "}"
        )
        df_qced = pd.DataFrame(response_data.json())
        df_qced = derived_ocean_variables(df_qced)
        df_qced = run_qc_profiles(
            df_qced, qartod_config=qartod_config, hakai_tests_config=hakai_tests_config
        )

        # Convert QARTOD to string temporarily
        qartod_columns = df_qced.filter(regex="_flag_level_1").columns
        # TODO Drop once qartod columns are of INT type in hakai DB
        df_qced[qartod_columns] = df_qced[qartod_columns].astype(str)
        df_qced = df_qced.replace({"": None})

        # Update qced casts stage and error log
        chunk["processing_stage"] = "9_qc_auto"
        chunk["process_error"] = chunk["process_error"].fillna("")
        # Upload to server
        if update_server:
            for _, row in tqdm(
                chunk.iterrows(), desc="Upload flags", unit="profil", total=len(chunk)
            ):
                logger.debug("Upload qced %s", row["hakai_id"])
                json_string = generate_process_flags_json(row, df_qced)
                response = client.post(
                    f"{api_root}/ctd/process/flags/json/{row['ctd_cast_pk']}",
                    json_string,
                )
                if response.status_code != 200:
                    print(f"Failed to update {row['hakai_id']}")
        else:
            qced_cast_data += [df_qced]

    return pd.concat(qced_cast_data), df_casts


def get_hakai_flag_columns(
    df,
    var,
    extra_flag_list="",
    flag_values_to_consider=[3, 4],
    level_1_flag_suffix="_flag_level_1",
    level_2_flag_suffix="_flag",
):
    """
    Generate the different Level1 and Level2 flag columns by grouping the different tests results.
    """

    def generate_level2_flag(row):
        """
        Regroup together tests results in "flag_value_to_consider" as a json string to be outputed as a level2 flag
        """
        level2 = [
            f"{hakai_tests.qartod_to_hakai_flag[value]}: {item}"
            for item, value in row.items()
            if value in flag_values_to_consider
        ]
        if level2 == {}:
            return ""
        else:
            return "; ".join(level2)

    # Retrieve each flags column associated to a variable
    var_flag_results = df.filter(regex=var + "_" + extra_flag_list)

    # Drop Hakai already existing flags, this will be dropped once we get the right flag columns
    #  available on the database side
    var_flag_results = var_flag_results.drop(var + "_flag", axis=1, errors="ignore")

    # Generete Level 1 Aggregated flag columns
    df[var + level_1_flag_suffix] = qartod_compare(
        var_flag_results.transpose().to_numpy()
    )

    # Generete Level 2 Flag Description for failed flag
    df[var + level_2_flag_suffix] = var_flag_results.apply(
        generate_level2_flag, axis="columns"
    )

    # Make sure that empty records are flagged as MISSING
    if var in df:
        df.loc[df[var].isna(), var + level_1_flag_suffix] = QartodFlags.MISSING
    return df


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--processed_cast_filter", default=None)
    parser.add_argument("--server", default="goose")
    parser.add_argument("--chunksize", default=100)
    parser.add_argument("--update_server", default=False)
    parser.add_argument("--qartod_config", default=None)
    parser.add_argument("--hakai_tests_config", default=None)
    args = parser.parse_args()

    # Run Query
    df = qc_hakai_profiles(**args.__dict__)
