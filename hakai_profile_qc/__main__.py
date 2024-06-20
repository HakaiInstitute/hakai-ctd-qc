import json
import logging
import os
import sys
from json import JSONDecodeError
from pathlib import Path

import click
import gsw
import numpy as np
import pandas as pd
import sentry_sdk
from dotenv import load_dotenv
from hakai_api import Client
from ioos_qc.config import Config
from ioos_qc.stores import PandasStore
from ioos_qc.streams import PandasStream
from sentry_sdk.crons import monitor
from sentry_sdk.integrations.logging import LoggingIntegration
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

from hakai_profile_qc import hakai_tests, sentry_warnings, variables
from hakai_profile_qc.version import __version__

load_dotenv()

QARTOD_DTYPE = pd.CategoricalDtype([9, 2, 1, 3, 4], ordered=True)


def check_hakai_database_rebuild(api_root):
    response = client.get(f"{api_root}/api/rebuild_status")
    is_running_rebuilding = response.json()[0]["rebuild_running"]
    if is_running_rebuilding:
        logger.warning(
            "Stop process early since Hakai DB %s is running a rebuild",
            api_root,
        )
        sys.exit()


def log_to_sentry():
    sentry_logging = LoggingIntegration(
        level=os.environ.get(
            "SENTRY_LEVEL", "INFO"
        ),  # Capture info and above as breadcrumbs
        event_level=os.environ.get(
            "SENTRY_EVENT_LEVEL", "WARNING"
        ),  # Send errors as events
    )
    sentry_sdk.init(
        dsn=os.environ.get("SENTRY_DSN"),
        integrations=[
            sentry_logging,
        ],
        environment=os.environ.get("ENVIRONMENT", "development"),
        release=f"hakai-profile-qc@{__version__}",
        traces_sample_rate=1.0,
    )


def run_profiling(output):
    import atexit
    import cProfile
    import io
    import pstats

    logger.info("Profiling...")
    pr = cProfile.Profile()
    pr.enable()

    def exit():
        pr.disable()
        print("Profiling completed")
        s = io.StringIO()
        pstats.Stats(pr, stream=s).sort_stats("cumulative").print_stats()
        with open(output, "w") as file:
            file.write(s.getvalue())

    atexit.register(exit)


tqdm.pandas()
PACKAGE_PATH = Path(__file__).parent
DEFAULT_CONFIG_PATH = PACKAGE_PATH / ".." / "default-config.yaml"
ENV_CONFIG_PATH = PACKAGE_PATH / ".." / "config.yaml"

HAKAI_TESTS_CONFIGURATION = json.loads(
    (PACKAGE_PATH / "config" / "hakai_ctd_profile_tests_config.json").read_text()
)
QARTOD_TESTS_CONFIGURATION = json.loads(
    (PACKAGE_PATH / "config" / "hakai_ctd_profile_qartod_test_config.json").read_text()
)
HAKAI_GREY_LIST = hakai_tests.load_grey_list(
    PACKAGE_PATH / "HakaiProfileDatasetGreyList.csv"
)

ioos_qc_coords_mapping = {
    "tinp": "measurement_dt",
    "zinp": "depth",
    "lon": "longitude",
    "lat": "latitude",
}


logger = logging.getLogger(__name__)
logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
log_to_sentry()
logger.info("Start Process")
if "HAKAI_API_TOKEN" in os.environ:
    logger.info(
        "HAKAI_API_TOKEN as env variable: %s", len(os.environ["HAKAI_API_TOKEN"])
    )
client = Client(credentials=os.environ.get("HAKAI_API_TOKEN"))


def get_hakai_station_list():
    """get_hakai_station_list
        Retrieve station list available within the Hakai production database.

    Returns:
        dataframe: full dataframe list of stations and
            associated depth, latitude, and longitude
    """
    return pd.DataFrame(
        client.get(
            "https://hecate.hakai.org/api/eims/views/output/sites?limit=-1"
        ).json()
    ).rename(columns={"name": "station", "depth": "station_depth"})


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


def run_qc_profiles(df, metadata):
    """
    Main method that runs on a number of profiles a series of QARTOD tests and specific
    to the Hakai CTD Dataset.
    """
    # Read configurations
    qartod_config = QARTOD_TESTS_CONFIGURATION
    hakai_tests_config = HAKAI_TESTS_CONFIGURATION

    # Regroup profiles by profile_id and direction and sort them along zinpQARTOD
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
                x, qartod_config, **ioos_qc_coords_mapping
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
                x, qartod_config, **ioos_qc_coords_mapping
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
    # Apply Query Based Flag
    if "query_based_flag" in hakai_tests_config:
        logger.debug("Run Query Based flag test")
        df = hakai_tests.query_based_flag_test(
            df, hakai_tests_config["query_based_flag"]
        )
    # Apply processing_log related flags
    df = hakai_tests.apply_flag_from_process_log(df, metadata)

    # APPLY QARTOD FLAGS FROM ONE CHANNEL TO OTHER AGGREGATED ONES
    # Generate Hakai Flags
    for var in tqdm(
        tested_variable_list, desc="Aggregate flags for each variables", unit="var"
    ):
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
    df = hakai_tests.grey_list(df, HAKAI_GREY_LIST)

    # Make sure that missing values and bad values are appropriately flagged
    for variable in df.columns:
        bad_value_flag = f"{variable}_hakai_bad_value_test"
        if bad_value_flag in df.columns:
            df.loc[df[bad_value_flag].isin([3, 4, 9]), f"{variable}_flag_level_1"] = (
                df.loc[df[bad_value_flag].isin([3, 4, 9]), bad_value_flag]
            )

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
    return pd.DataFrame()


@click.command()
@click.option("--hakai_ids", help="Comma delimited list of hakai_ids to qc", type=str)
@click.option(
    "--processing-stages",
    help="Comma list of processing_stage profiles to review [env=QC_PROCESSING_STAGES]",
    default="8_binAvg,8_rbr_processed",
    show_default=True,
    envvar="QC_PROCESSING_STAGES",
)
@click.option(
    "--test-suite",
    help="Run Test suite [env=RUN_TEST_SUITE]",
    is_flag=True,
    default=False,
    envvar="RUN_TEST_SUITE",
)
@click.option(
    "--api-root",
    help="Hakai API root to use [env=HAKAI_API_SERVER_ROOT]",
    default="https://goose.hakai.org/api",
    show_default=True,
    envvar="HAKAI_API_SERVER_ROOT",
)
@click.option(
    "--upload-flag",
    help="Update database flags [env=UPDATE_SERVER_DATABASE]",
    default=False,
    is_flag=True,
    show_default=True,
    envvar="UPDATE_SERVER_DATABASE",
)
@click.option(
    "--chunksize",
    help="Process profiles by chunk [env=CTD_CAST_CHUNKSIZE]",
    type=int,
    default=100,
    show_default=True,
    envvar="CTD_CAST_CHUNKSIZE",
)
@click.option(
    "--sentry-minimum-date",
    type=click.DateTime(),
    help="Minimum date to use to generate sentry warnings [env=SENTRY_MINIMUM_DATE]",
    default=None,
    envvar="SENTRY_MINIMUM_DATE",
)
@click.option("--profile", type=click.Path(), default=None, help="Run cProfile")
@monitor(monitor_slug=os.getenv("SENTRY_MONITOR_ID"))
def main(
    hakai_ids,
    test_suite,
    api_root,
    upload_flag,
    processing_stages,
    chunksize,
    sentry_minimum_date,
    profile,
):
    """QC Hakai Profiles on subset list of profiles given either via an
    hakai_id list, the `test_suite` flag or processing_stage.
    If no input is given, the tool will default to qc all the profiles
    that have been processed but not qced yet:
        processing_stage={8_binAvg,8_rbr_processed}

    Each options can be defined either as an argument
    or via the associated environment variable.
    """

    check_hakai_database_rebuild(api_root)
    if profile:
        run_profiling(profile)
    #  Generate filter query list based on input and configuration
    if hakai_ids:
        run_type = f"hakai_ids={hakai_ids}"
        sentry_sdk.set_tag("process", "special query")
        if isinstance(hakai_ids, list):
            hakai_ids = ",".join(hakai_ids)
        logger.info("Run QC on hakai_ids: %s", hakai_ids)
        cast_filter_query = "hakai_id={%s}" % hakai_ids
    elif test_suite:
        run_type = "Test Suite"
        logger.info("Running test suite")
        cast_filter_query = "hakai_id={%s}" % ",".join(variables.HAKAI_TEST_SUITE)
    else:
        logger.info("Run QC on processing stages: %s", processing_stages)
        cast_filter_query = "processing_stage={%s}" % processing_stages
        run_type = cast_filter_query

    # Create warning if rebuild
    if "8_binAvg,8_rbr_processed,9_qc_auto,10_qc_pi" in run_type:
        logger.warning("Full CTD QC rebuild is started on %s", api_root)

    # Retrieve casts to qc
    url = f"{api_root}/ctd/views/file/cast?{cast_filter_query}&limit=-1&fields={','.join(variables.CTD_CAST_VARIABLES)}"
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
        for chunk in np.array_split(df_casts, np.ceil(len(df_casts) / chunksize)):
            # Retrieve cast data for this chunk
            query = "%s/ctd/views/file/cast/data?hakai_id={%s}&limit=-1&fields=%s" % (
                api_root,
                ",".join(chunk["hakai_id"].values),
                ",".join(variables.CTD_CAST_DATA_VARIABLES),
            )
            metadata_query = "%s/ctd/views/file/cast?hakai_id={%s}&limit=-1" % (
                api_root,
                ",".join(chunk["hakai_id"].values),
            )

            logger.debug("Run query: %s", query)
            df_qced = retrieve_hakai_data(query, max_attempts=3)
            metadata = retrieve_hakai_data(metadata_query, max_attempts=3)
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
            df_qced = run_qc_profiles(df_qced, metadata)
            if sentry_minimum_date:
                sentry_warnings.run_sentry_warnings(df_qced, chunk, sentry_minimum_date)

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
            if upload_flag:
                # Filter out extra variables generated during qc
                df_upload = df_qced[original_variables]
                logger.info("Upload results to %s", api_root)
                for _, row in chunk.iterrows():
                    retrieve_hakai_data(
                        f"{api_root}/ctd/process/flags/json/{row['ctd_cast_pk']}",
                        post=_generate_process_flags_json(row, df_upload),
                    )
            else:
                logger.info("Do not upload results to %s", api_root)

            gen_pbar.update(n=len(chunk))
            logger.debug("Chunk processed")

    if "8_binAvg,8_rbr_processed,9_qc_auto,10_qc_pi" in run_type:
        logger.warning("Full CTD QC rebuild is completed on %s", api_root)


def _get_hakai_flag_columns(
    df,
    variable,
    flag_regex="",
):
    """
    Generate the different Level1 and Level2 flag columns by
    grouping the different tests results.
    """

    def __generate_level2_flag(row: pd.Series):
        """
        Regroup together tests results in "flag_value_to_consider" as a
        json string to be outputed as a level2 flag
        """
        flags = row.dropna().to_dict()
        if not flags:
            return None
        return "; ".join(
            sorted(
                [
                    f"{hakai_tests.qartod_to_hakai_flag[qartod_flag]}: {test}"
                    for test, qartod_flag in flags.items()
                ],
                reverse=True,
            )
        )

    # Retrieve each flags column associated to a variable
    df_subset = (
        df.filter(regex=flag_regex).replace({9: None, 2: None}).dropna(how="all")
    )
    if df_subset.empty:
        return df

    if f"{variable}_flag" in df_subset:
        raise RuntimeError(
            "Variable grouped flag considered in flag columns to compare"
        )
    if f"{variable}_flag_level_1" in df_subset:
        raise RuntimeError(
            "Variable flag level 1 considered in flag columns to compare"
        )

    # Generete Level 1 Aggregated flag columns
    logger.debug("Get Aggregated QARTOD Level 1 Flags")
    df.loc[df_subset.index, variable + "_flag_level_1"] = (
        df_subset.astype(QARTOD_DTYPE).max(axis=1).astype(int)
    )
    logger.debug("Get Aggregated Hakai Flags")
    # Generete Level 2 Flag Description for failed flag
    df.loc[df_subset.index, variable + "_flag"] = df_subset.replace({1: None}).apply(
        lambda x: __generate_level2_flag(x),
        axis=1,
    )
    return df


if __name__ == "__main__":
    main()
    sys.exit(0)
