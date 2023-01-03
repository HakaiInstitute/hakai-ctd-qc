import argparse
import json
import logging
import os
from time import time
from requests.exceptions import JSONDecodeError
import sys

import gsw
import numpy as np
import pandas as pd
import sentry_sdk
import yaml
from hakai_api import Client
from ioos_qc.config import Config
from ioos_qc.qartod import QartodFlags, qartod_compare
from ioos_qc.stores import PandasStore
from ioos_qc.streams import PandasStream
from ocean_data_parser.read.utils import standardize_dataset
from sentry_sdk.integrations.logging import LoggingIntegration
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

import hakai_tests
import sentry_warnings
from version import __version__

start_time = time()


def log_to_sentry():
    if config["SENTRY_DSN"] is None:
        return
    sentry_logging = LoggingIntegration(
        level=logging.getLevelName(
            config["SENTRY_LEVEL"]
        ),  # Capture info and above as breadcrumbs
        event_level=logging.getLevelName(
            config["SENTRY_EVENT_LEVEL"]
        ),  # Send errors as events
    )
    sentry_sdk.init(
        dsn=config["SENTRY_DSN"],
        integrations=[
            sentry_logging,
        ],
        environment=config["ENVIRONMENT"],
        release=f"hakai-profile-qc@{__version__}",
        traces_sample_rate=1.0,
    )


tqdm.pandas()
PACKAGE_PATH = os.path.join(os.path.dirname(__file__))
DEFAULT_CONFIG_PATH = os.path.join(PACKAGE_PATH, "..", "default-config.yaml")
ENV_CONFIG_PATH = os.path.join(PACKAGE_PATH, "..", "config.yaml")
config_from_env = [
    "HAKAI_API_TOKEN",
    "ENVIRONMENT",
    "HAKAI_API_SERVER_ROOT",
    "UPDATE_SERVER_DATABASE",
    "SENTRY_EVENT_MINIMUM_DATE",
    "LOGGING_LEVEL",
    "RUN_TEST_SUITE",
    "QC_PROCESSING_STAGES",
]
minimum_cast_variables = [
    "ctd_cast_pk",
    "hakai_id",
    "processing_stage",
    "process_error",
] + sentry_warnings.context


def read_config_yaml():
    """YAML Config reader replace any ${PACKAGE_PATH} by the package actual path"""

    def __parse_config_yaml(config_path):
        with open(config_path, encoding="UTF-8") as f:
            yaml_str = f.read()
        yaml_str = yaml_str.replace("${PACKAGE_PATH}", PACKAGE_PATH)
        return yaml.load(yaml_str, Loader=yaml.SafeLoader)

    # Read config from the different sources
    parsed_config = __parse_config_yaml(DEFAULT_CONFIG_PATH)
    if os.path.exists(ENV_CONFIG_PATH):
        parsed_config.update(__parse_config_yaml(ENV_CONFIG_PATH))
    # environment variables
    parsed_config.update(
        {key: os.environ[key] for key in config_from_env if key in os.environ}
    )

    # Parse input files
    if "QARTOD_TESTS_CONFIGURATION_PATH" in parsed_config:
        with open(
            parsed_config["QARTOD_TESTS_CONFIGURATION_PATH"], encoding="UTF-8"
        ) as f:
            parsed_config["qartod_tests_config"] = json.load(f)
    if "HAKAI_TESTS_CONFIGURATION_PATH" in parsed_config:
        with open(
            parsed_config["HAKAI_TESTS_CONFIGURATION_PATH"], encoding="UTF-8"
        ) as f:
            parsed_config["hakai_tests_config"] = json.load(f)
    if "HAKAI_CTD_ATTRIBUTES" in parsed_config:
        with open(parsed_config["HAKAI_CTD_ATTRIBUTES"], encoding="UTF-8") as f:
            parsed_config["netcdf_attributes"] = json.load(f)
    if "SENTRY_EVENT_MINIMUM_DATE" in parsed_config:
        parsed_config["SENTRY_EVENT_MINIMUM_DATE"] = pd.to_datetime(
            parsed_config["SENTRY_EVENT_MINIMUM_DATE"]
        )
    return parsed_config


config = read_config_yaml()
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=config["LOGGING_LEVEL"],
    format=config["LOGGING_FORMAT"],
)
log_to_sentry()
logger.info("Start Process")
# Log to Hakai
client = Client(credentials=config.get("HAKAI_API_TOKEN"))


def _run_ioosqc_on_dataframe(df, qc_config, tinp="t", zinp="z", lat="lat", lon="lon"):
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


def _generate_process_flags_json(cast, data):
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


def _derived_ocean_variables(df):
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


def _convert_time_to_datetime(df):
    time_vars = ["start_dt", "bottom_dt", "end_dt", "measurement_dt"]
    for time_var in time_vars:
        df[time_var] = pd.to_datetime(df[time_var], utc=True)
    return df


def run_qc_profiles(df):
    """
    Main method that runs on a number of profiles a series of QARTOD tests and specific
    to the Hakai CTD Dataset.
    """
    # Read configurations
    # QARTOD
    qartod_config = config["qartod_tests_config"]
    hakai_tests_config = config["hakai_tests_config"]

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
        logger.debug(
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
        .groupby(["hakai_id", "direction_flag"], as_index=False, group_keys=True)
        .progress_apply(
            lambda x: _run_ioosqc_on_dataframe(
                x, qartod_config, **config["ioos_qc_coords_mapping"]
            ),
        )
    )
    # On static measurements
    tqdm.pandas(
        desc="Apply QARTOD Tests to individual static measurements",
        unit=" measurement",
    )
    # Drop QARTOD tests that aren't compatible with static unique mesurements
    static_qartod_config = qartod_config.copy()
    for context in static_qartod_config["contexts"]:
        for var, tests in context["streams"].items():
            tests["qartod"].pop("attenuated_signal_test", None)
    df_static = (
        df.query("direction_flag in ('s')")
        .groupby(["hakai_id", "measurement_dt"], as_index=False, group_keys=True)
        .progress_apply(
            lambda x: _run_ioosqc_on_dataframe(
                x, qartod_config, **config["ioos_qc_coords_mapping"]
            ),
        )
    )

    # Regroup back together profiles and static data
    df = pd.concat([df_profiles, df_static]).reset_index(drop=True)

    # HAKAI SPECIFIC TESTS #
    # This section regroup different non QARTOD tests which are specific to
    # Hakai profile dataset. Most of the them
    # uses the pandas dataframe to transform the data and apply divers tests.
    # DO CAP DETECTION
    logger.info("Apply Hakai Specific Tests")
    if "do_cap_test" in hakai_tests_config:
        for key in hakai_tests_config["do_cap_test"]["variable"]:
            logger.debug("DO Cap Detection to %s variable", key)
            df = hakai_tests.do_cap_test(
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
    #  Find Profiles that were flagged near the bottom and assume this is
    # likely related to having it the bottom.
    if "bottom_hit_detection" in hakai_tests_config:
        logger.debug("Flag Bottom Hit Data")
        df = hakai_tests.bottom_hit_detection(
            df,
            variables=hakai_tests_config["bottom_hit_detection"]["variable"],
            profile_id="hakai_id",
            depth_variable="depth",
            profile_direction_variable="direction_flag",
        )

    # Detect PAR Shadow
    if "par_shadow_test" in hakai_tests_config:
        logger.debug("Flag PAR Shadow Data")
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
    # Station Maximum Depth Test
    if "depth_range_test" in hakai_tests_config:
        logger.debug("Review maximum depth per profile vs station")
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

    # APPLY QARTOD FLAGS FROM ONE CHANNEL TO OTHER AGGREGATED ONES
    # Generate Hakai Flags
    for var in tested_variable_list:
        logger.debug("Apply flag results to %s", var)

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
        df = _get_hakai_flag_columns(df, var, extra_flags)

    # Apply Hakai Grey List
    # Grey List should overwrite the QARTOD Flags
    logger.debug("Apply Hakai Grey List")
    df = hakai_tests.grey_list(df)
    return df


def retrieve_hakai_data(url, post=None, max_attempts: int = 3):
    """Run query to hakai api and return a pandas dataframe if sucessfull.
    A minimum of attemps (default: 3) will be tried if the query fails."""
    attempts = 0
    while attempts < max_attempts:
        if post:
            response_data = client.post(url, post)
        else:
            response_data = client.get(url)

        if response_data.status_code != 200:
            logger.warning(
                "ERROR %s Failed to retrieve profile data from hakai server. Lets try again: %s",
                response_data.status_code,
                response_data.text,
            )
            attempts += 1
            continue
        elif post:
            # sucessfull post to server
            return

        # attempt to read json return
        try:
            logger.info("Load to dataframe response.json")
            return pd.DataFrame(response_data.json())

        except JSONDecodeError:
            logger.error(
                "Failed to decode json data for this query: %s", url, exc_info=True
            )
            attempts += 1
            continue

    logger.error(
        "Reached the maximum number of attemps to retrieve data from the hakai server: %s",
        url,
    )


def main(hakai_ids=None):
    """Run Hakai Profile

    Args:
        hakai_ids (str): list of hakai_ids to run qc on. If None run by defined processing stages

    Returns:
        (ctd_cast_data,ctd_cast): Resulting data as pandas dataframes.
    """

    # Get the list of hakai_ids to qc
    if hakai_ids:
        if isinstance(hakai_ids, list):
            hakai_ids = ",".join(hakai_ids)
        logger.info("Run QC on hakai_ids: %s", hakai_ids)
        cast_filter_query = "hakai_id={%s}" % hakai_ids
    elif config["RUN_TEST_SUITE"] or config["RUN_TEST_SUITE"] not in ["false", "False"]:
        logger.info("Running test suite")
        cast_filter_query = "hakai_id={%s}" % ",".join(config["TEST_HAKAI_IDS"])
    else:
        processing_stages = ",".join(config["QC_PROCESSING_STAGES"])
        logger.info("Run QC on processing stages: %s", processing_stages)
        cast_filter_query = "processing_stage={%s}" % processing_stages

    url = f"{config['HAKAI_API_SERVER_ROOT']}/{config['CTD_CAST_ENDPOINT']}?{cast_filter_query}&limit=-1&fields={','.join(minimum_cast_variables)}"
    logger.info("Retrieve: %s", url)
    df_casts = retrieve_hakai_data(url, max_attempts=3)
    if df_casts.empty:
        logger.info("No Drops needs to be QC")
        return None, None
    logger.info("QC %s drops", len(df_casts))
    gen_pbar = tqdm(
        total=len(df_casts),
        desc="Profiles to qc",
        unit="profiles",
        colour="GREEN",
        dynamic_ncols=True,
        ncols=100,
    )
    profile_processed = 0
    with logging_redirect_tqdm():
        for chunk in np.array_split(
            df_casts, np.ceil(len(df_casts) / config["CTD_CAST_CHUNKSIZE"])
        ):
            logger.debug("QC hakai_ids: %s", str(chunk["hakai_id"]))
            logger.debug(
                "Retrieve data from hakai server: %s/%s profile qced",
                profile_processed,
                len(df_casts),
            )
            query = "%s/%s?hakai_id={%s}&limit=-1&fields=%s" % (
                config["HAKAI_API_SERVER_ROOT"],
                config["CTD_CAST_DATA_ENDPOINT"],
                ",".join(chunk["hakai_id"].values),
                ",".join(config["CTD_VARIABLES"]),
            )
            logger.info("Retrieve profiles data from hakai server")
            logger.debug("Run query: %s", query)
            df_qced = retrieve_hakai_data(query, max_attempts=3)
            if df_qced is None:
                logger.error(
                    "Failed to retrieve profile data for the hakai_ids: %s",
                    chunk["hakai_id"],
                )
                continue

            logger.info("Data is loaded")
            original_variables = df_qced.columns
            logger.debug("Generate derived variables")
            df_qced = _derived_ocean_variables(df_qced)
            logger.debug("Convert time variables to datetime objects")
            df_qced = _convert_time_to_datetime(df_qced)
            logger.debug("Run QC Process")
            df_qced = run_qc_profiles(df_qced)

            sentry_warnings.run_sentry_warnings(
                df_qced, chunk, config["SENTRY_EVENT_MINIMUM_DATE"]
            )

            # Convert QARTOD to string temporarily
            qartod_columns = df_qced.filter(regex="_flag_level_1").columns
            # TODO Drop once qartod columns are of INT type in hakai DB
            df_qced[qartod_columns] = df_qced[qartod_columns].astype(str)
            df_qced = df_qced.replace({"": None})

            # Update qced casts processing_stage
            chunk["processing_stage"] = chunk["processing_stage"].replace(
                {" 8_binAvg": "9_qc_auto", "8_rbr_processed": "9_qc_auto"}
            )
            chunk["process_error"] = chunk["process_error"].fillna("")

            # Upload to server
            if config["UPDATE_SERVER_DATABASE"] in (True, "true"):
                for _, row in tqdm(
                    chunk.iterrows(),
                    desc=f"Upload flags to {config['HAKAI_API_SERVER_ROOT']}",
                    unit="profil",
                    total=len(chunk),
                ):
                    logger.debug("Upload qced %s", row["hakai_id"])
                    json_string = _generate_process_flags_json(
                        row, df_qced[original_variables]
                    )
                    retrieve_hakai_data(
                        f"{config['HAKAI_API_SERVER_ROOT']}/ctd/process/flags/json/{row['ctd_cast_pk']}",
                        post=json_string,
                    )
            profile_processed += len(chunk)
            gen_pbar.update(n=profile_processed)
            logger.info("Chunk processed")


def _get_hakai_flag_columns(
    df,
    var,
    extra_flag_list="",
    flag_values_to_consider=None,
    level_1_flag_suffix="_flag_level_1",
    level_2_flag_suffix="_flag",
):
    """
    Generate the different Level1 and Level2 flag columns by grouping the different tests results.
    """

    def __generate_level2_flag(row):
        """
        Regroup together tests results in "flag_value_to_consider" as a json string to be outputed as a level2 flag
        """
        level2 = [
            f"{hakai_tests.qartod_to_hakai_flag[value]}: {item}"
            for item, value in row.items()
            if value in flag_values_to_consider
        ]
        return "; ".join(level2) if level2 != {} else ""

    if flag_values_to_consider is None:
        flag_values_to_consider = [3, 4]

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
        __generate_level2_flag, axis="columns"
    )

    # Make sure that empty records are flagged as MISSING
    if var in df:
        df.loc[df[var].isna(), var + level_1_flag_suffix] = QartodFlags.MISSING
    return df


def _generate_netcdf_attributes(ds):

    for var in ds:
        if var == "direction_flag":
            ds[var].attrs["flag_values"] = config["FLAG CONVENTION"][
                "direction_flag"
            ].keys()
            ds[var].attrs["flag_meaning"] = " ".join(
                config["FLAG CONVENTION"]["direction_flag"].values()
            )
        elif var.endswith("_flag"):
            ds[var].attrs["flag_values"] = config["FLAG CONVENTION"]["Hakai"].keys()
            ds[var].attrs["flag_meaning"] = " ".join(
                config["FLAG CONVENTION"]["Hakai"].values()
            )
        elif var.endswith("_flag_level_1"):
            ds[var].attrs["flag_values"] = config["FLAG CONVENTION"]["QARTOD"].keys()
            ds[var].attrs["flag_meaning"] = " ".join(
                config["FLAG CONVENTION"]["QARTOD"].values()
            )
    return ds


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--hakai_ids", help="comma separated list of hakai_ids to process", default=None
    )
    parser.add_argument(
        "--config",
        help="json dictionary configuration to pass and overwrite default",
        default=None,
    )
    args = parser.parse_args()
    if args.config:
        config.update(json.loads(args.config))

    # Run Query
    if args.hakai_ids:
        sentry_sdk.set_tag("process", "special query")
        df = main(hakai_ids=args.hakai_ids)
    else:
        main()


end_time = time()
logger.info("Process completed in %s seconds", end_time - start_time)
sys.exit(0)
