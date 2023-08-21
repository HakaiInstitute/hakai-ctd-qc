import argparse
import json
import logging
import os
import sys
from json import JSONDecodeError
from time import time

import gsw
import numpy as np
import pandas as pd
import requests
import sentry_sdk
import yaml
from hakai_api import Client
from ioos_qc.config import Config
from ioos_qc.qartod import qartod_compare
from ioos_qc.stores import PandasStore
from ioos_qc.streams import PandasStream
from sentry_sdk.integrations.logging import LoggingIntegration
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

from hakai_profile_qc import hakai_tests, sentry_warnings
from hakai_profile_qc.version import __version__

sentry_checkin_headers = {
    "Authorization": "DSN https://ab3a1d65934a460bbd350f7d48a931d4@o56764.ingest.sentry.io/6685251"
}
monitor_id = "8ac7c3da-4e18-4c7b-9ce9-c0fa22956775"  # Write your monitor_id here

# Create the check-in
if __name__ == "__main__":
    sentry_health_response = requests.post(
        f"https://sentry.io/api/0/monitors/{monitor_id}/checkins/",
        headers=sentry_checkin_headers,
        json={"status": "in_progress"},
    )
    check_in_id = sentry_health_response.json()["id"]

    start_time = time()

qartod_dtype = pd.CategoricalDtype([9, 2, 1, 3, 4], ordered=True)


def check_hakai_database_rebuild():
    response = client.get(f"{config['HAKAI_API_SERVER_ROOT']}/api/rebuild_status")
    is_running_rebuilding = response.json()[0]["rebuild_running"]
    if is_running_rebuilding:
        logger.warning(
            "Stop process early since Hakai DB %s is running a rebuild",
            config["HAKAI_API_SERVER_ROOT"],
        )
        requests.put(
            f"https://sentry.io/api/0/monitors/{monitor_id}/checkins/{check_in_id}/",
            headers=sentry_checkin_headers,
            json={"status": "ok"},
        )
        sys.exit()


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


def read_config_yaml():
    """
    YAML Config reader replace any ${PACKAGE_PATH} by the
    package actual path
    """

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
        {key: os.environ[key] for key in os.environ if key in parsed_config}
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
logger.info("ENVIRONMENT_VARIABLES: %s", dict(os.environ).keys())
if "HAKAI_API_TOKEN" in os.environ:
    logger.info(
        "HAKAI_API_TOKEN as env variable: %s", len(os.environ["HAKAI_API_TOKEN"])
    )
logger.info(
    "HAKAI_API_TOKEN: len(%s)",
    len(config["HAKAI_API_TOKEN"]) if config["HAKAI_API_TOKEN"] else "none",
)
logger.debug("config: %s", config)
client = Client(credentials=config.get("HAKAI_API_TOKEN"))
check_hakai_database_rebuild()


def get_hakai_station_list():
    """get_hakai_station_list
        Retrieve station list available within the Hakai production database.

    Returns:
        dataframe: full dataframe list of stations and
            associated depth, latitude, and longitude
    """
    client = Client(credentials=config.get("HAKAI_API_TOKEN"))
    response = client.get(
        "https://hecate.hakai.org/api/eims/views/output/sites?limit=-1"
    )
    return pd.DataFrame(response.json()).rename(
        columns={"name": "station", "depth": "station_depth"}
    )


hakai_stations = get_hakai_station_list()


def _run_ioosqc_on_dataframe(df, qc_config, tinp="t", zinp="z", lat="lat", lon="lon"):
    """
    Apply ioos_qc configuration to a Pandas DataFrame by using the
    PandasStream mand PandasStore methods from ioos_qc
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

    def _drop_sbe_flag(x):
        return x.replace({-9.99e-29: np.nan})

    longitude = df["station_longitude"].fillna(df["longitude"])
    latitude = df["station_latitude"].fillna(df["latitude"])
    df["absolute salinity"] = gsw.SA_from_SP(
        _drop_sbe_flag(df["salinity"]),
        _drop_sbe_flag(df["pressure"]),
        longitude,
        latitude,
    )
    df["conservative temperature"] = gsw.CT_from_t(
        df["absolute salinity"],
        _drop_sbe_flag(df["temperature"]),
        df["pressure"],
    )
    df["density"] = gsw.rho(
        df["absolute salinity"],
        df["conservative temperature"],
        _drop_sbe_flag(df["pressure"]),
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
        df = hakai_tests.bad_value_test(
            df,
            **hakai_tests_config["bad_value_test"],
        )
        # Replace all bad values by np.nan
        df = df.replace({value: np.nan for value in [None, pd.NA, -9.99e-29]})

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
        for key in hakai_tests_config["do_cap_test"].pop("variable", []):
            logger.debug("DO Cap Detection to %s variable", key)
            df = hakai_tests.do_cap_test(
                df,
                key,
                **hakai_tests_config["do_cap_test"],
            )

    # BOTTOM HIT DETECTION
    #  Find Profiles that were flagged near the bottom and assume this is
    # likely related to having it the bottom.
    if "bottom_hit_detection" in hakai_tests_config:
        logger.debug("Flag Bottom Hit Data")
        df = hakai_tests.bottom_hit_detection(
            df, **hakai_tests_config["bottom_hit_detection"]
        )

    # Detect PAR Shadow
    if "par_shadow_test" in hakai_tests_config:
        logger.debug("Flag PAR Shadow Data")
        df = hakai_tests.par_shadow_test(
            df,
            **hakai_tests_config["par_shadow_test"],
        )
    # Station Maximum Depth Test
    if "depth_range_test" in hakai_tests_config:
        logger.debug("Review maximum depth per profile vs station")
        df = hakai_tests.hakai_station_maximum_depth_test(
            df, hakai_stations, **hakai_tests_config["depth_range_test"]
        )

    if "query_based_flag" in hakai_tests_config:
        logger.debug("Run Query Based flag test")
        df = hakai_tests.query_based_flag_test(
            df, hakai_tests_config["query_based_flag"]
        )
    # APPLY QARTOD FLAGS FROM ONE CHANNEL TO OTHER AGGREGATED ONES
    # Generate Hakai Flags
    for var in tested_variable_list:
        logger.debug("Apply flag results to %s", var)
        consirederd_flag_columns = "|".join(
            hakai_tests_config["flag_aggregation"]["default"]
            + hakai_tests_config["flag_aggregation"].get(var, [])
            + [f"{var}_qartod_.*|{var}_hakai_.*"]
        )
        df = _get_hakai_flag_columns(df, var, consirederd_flag_columns)

    # Apply Hakai Grey List
    # Grey List should overwrite the QARTOD Flags
    logger.debug("Apply Hakai Grey List")
    df = hakai_tests.grey_list(df)

    # Make sure that missing values and bad values are appropriately flagged
    for variable in df.columns:
        bad_value_flag = f"{variable}_hakai_bad_value_test"
        if bad_value_flag in df.columns:
            df.loc[
                df[bad_value_flag].isin([3, 4, 9]), f"{variable}_flag_level_1"
            ] = df.loc[df[bad_value_flag].isin([3, 4, 9]), bad_value_flag]

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

    #  Generate filter query list based on input and configuration
    if hakai_ids:
        run_type = f"hakai_ids={hakai_ids}"
        if isinstance(hakai_ids, list):
            hakai_ids = ",".join(hakai_ids)
        logger.info("Run QC on hakai_ids: %s", hakai_ids)
        cast_filter_query = "hakai_id={%s}" % hakai_ids
    elif config["RUN_TEST_SUITE"] and config["RUN_TEST_SUITE"] not in [
        "false",
        "False",
    ]:
        run_type = "Test Suite"
        logger.info("Running test suite")
        cast_filter_query = "hakai_id={%s}" % ",".join(config["TEST_HAKAI_IDS"])
    else:
        processing_stages = config["QC_PROCESSING_STAGES"]
        logger.info("Run QC on processing stages: %s", processing_stages)
        cast_filter_query = "processing_stage={%s}" % processing_stages
        run_type = cast_filter_query

    # Create warning if rebuild
    if "8_binAvg,8_rbr_processed,9_qc_auto,10_qc_pi" in run_type:
        logger.warning(
            "Full CTD QC rebuild is started on %s", config["HAKAI_API_SERVER_ROOT"]
        )

    # Retrieve casts to qc
    url = f"{config['HAKAI_API_SERVER_ROOT']}/{config['CTD_CAST_ENDPOINT']}?{cast_filter_query}&limit=-1&fields={','.join(config['CTD_CAST_VARIABLES'])}"
    logger.info("Retrieve: %s", url)
    df_casts = retrieve_hakai_data(url, max_attempts=3)
    if df_casts.empty:
        logger.info("No Drops needs to be QC")
        return None, None

    # Split cast list to qc into chunks and run qc tests on each chunks.
    logger.info("QC %s drops", len(df_casts))
    gen_pbar = tqdm(
        total=len(df_casts),
        desc="Profiles to qc",
        unit="profiles",
        colour="GREEN",
        dynamic_ncols=True,
        ncols=100,
    )
    with logging_redirect_tqdm():
        for chunk in np.array_split(
            df_casts, np.ceil(len(df_casts) / config["CTD_CAST_CHUNKSIZE"])
        ):
            # Retrieve cast data for this chunk
            query = "%s/%s?hakai_id={%s}&limit=-1&fields=%s" % (
                config["HAKAI_API_SERVER_ROOT"],
                config["CTD_CAST_DATA_ENDPOINT"],
                ",".join(chunk["hakai_id"].values),
                ",".join(config["CTD_CAST_DATA_VARIABLES"]),
            )
            logger.debug("Run query: %s", query)
            df_qced = retrieve_hakai_data(query, max_attempts=3)
            original_variables = df_qced.columns
            if df_qced is None:
                logger.error(
                    "Failed to retrieve profile data for the hakai_ids: %s",
                    chunk["hakai_id"],
                )
                continue

            # Generate derived variables and convert time
            df_qced = _derived_ocean_variables(df_qced)
            df_qced = _convert_time_to_datetime(df_qced)

            # Run QC Process
            logger.debug("Run QC Process")
            df_qced = run_qc_profiles(df_qced)
            if config.get("SENTRY_RUN_WARNINGS"):
                sentry_warnings.run_sentry_warnings(
                    df_qced, chunk, config["SENTRY_EVENT_MINIMUM_DATE"]
                )

            # Convert QARTOD to string temporarily
            qartod_columns = df_qced.filter(regex="_flag_level_1").columns
            df_qced[qartod_columns] = df_qced[qartod_columns].astype(str)
            df_qced = df_qced.replace({"": None})

            # Update qced casts processing_stage
            chunk["processing_stage"] = chunk["processing_stage"].replace(
                {"8_binAvg": "9_qc_auto", "8_rbr_processed": "9_qc_auto"}
            )
            chunk["process_error"] = chunk["process_error"].fillna("")

            # Upload to server
            if config["UPDATE_SERVER_DATABASE"] in (True, "true"):
                # Filter out extra variables generated during qc
                df_upload = df_qced[original_variables]
                logger.debug("Upload results to %s", config["HAKAI_API_SERVER_ROOT"])
                for _, row in chunk.iterrows():
                    json_string = _generate_process_flags_json(row, df_upload)
                    retrieve_hakai_data(
                        f"{config['HAKAI_API_SERVER_ROOT']}/ctd/process/flags/json/{row['ctd_cast_pk']}",
                        post=json_string,
                    )
            gen_pbar.update(n=len(chunk))
            logger.debug("Chunk processed")

    if "8_binAvg,8_rbr_processed,9_qc_auto,10_qc_pi" in run_type:
        logger.warning(
            "Full CTD QC rebuild is completed on %s", config["HAKAI_API_SERVER_ROOT"]
        )


def _get_hakai_flag_columns(
    df,
    var,
    extra_flag_list="",
    flag_values_to_consider=None,
    level_1_flag_suffix="_flag_level_1",
    level_2_flag_suffix="_flag",
):
    """
    Generate the different Level1 and Level2 flag columns by
    grouping the different tests results.
    """

    def __generate_level2_flag(row):
        """
        Regroup together tests results in "flag_value_to_consider" as a
        json string to be outputed as a level2 flag
        """
        return (
            "; ".join(
                [
                    f"{hakai_tests.qartod_to_hakai_flag[flag]}: {test}"
                    for test, flag in row.astype(qartod_dtype)
                    .sort_values(ascending=False)
                    .dropna()
                    .items()
                ]
            )
            or None
        )

    if flag_values_to_consider is None:
        flag_values_to_consider = [3, 4]

    # Retrieve each flags column associated to a variable
    var_flag_results = df.filter(regex=extra_flag_list)

    if f"{var}_flag" in var_flag_results:
        raise RuntimeError(
            "Variable grouped flag considered in flag columns to compare"
        )
    if f"{var}_flag_level_1" in var_flag_results:
        raise RuntimeError(
            "Variable flag level 1 considered in flag columns to compare"
        )

    # Generete Level 1 Aggregated flag columns
    df[var + level_1_flag_suffix] = qartod_compare(
        var_flag_results.transpose().to_numpy()
    )

    # Generete Level 2 Flag Description for failed flag
    df[var + level_2_flag_suffix] = (
        var_flag_results.astype(qartod_dtype)
        .replace({9: None, 2: None, 1: None})
        .apply(
            __generate_level2_flag,
            axis=1,
        )
    )
    return df


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
    # Update the check-in status (required) and duration (optional)
    sentry_health_response = requests.put(
        f"https://sentry.io/api/0/monitors/{monitor_id}/checkins/{check_in_id}/",
        headers=sentry_checkin_headers,
        json={"status": "ok"},
    )
    sys.exit(0)
