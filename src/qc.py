import argparse
import json
import logging
import os
from re import L
import yaml

import gsw
import numpy as np
import pandas as pd
from hakai_api import Client
from ioos_qc.config import Config
from ioos_qc.qartod import QartodFlags, qartod_compare
from ioos_qc.stores import PandasStore
from ioos_qc.streams import PandasStream
from tqdm import tqdm
import sentry_sdk
from sentry_sdk.integrations.logging import LoggingIntegration
from ocean_data_parser.read.utils import standardize_dateset

import hakai_tests


logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s",
)
sentry_logging = LoggingIntegration(
    level=logging.INFO,  # Capture info and above as breadcrumbs
    event_level=logging.WARNING,  # Send errors as events
)
# sentry_sdk.init(
#     dsn="https://ab3a1d65934a460bbd350f7d48a931d4@o56764.ingest.sentry.io/6685251",
#     integrations=[
#         sentry_logging,
#     ],
#     traces_sample_rate=1.0,
# )


tqdm.pandas()
PACKAGE_PATH = os.path.join(os.path.dirname(__file__))
DEFAULT_CONFIG_PATH = os.path.join(PACKAGE_PATH, "config", "config.yaml")


def read_config_yaml(path):
    """YAML Config reader replace any ${PACKAGE_PATH} by the package actual path"""
    with open(os.path.join(path), encoding="UTF-8") as f:
        yaml_str = f.read()
    yaml_str = yaml_str.replace("${PACKAGE_PATH}", PACKAGE_PATH)
    config = yaml.load(yaml_str, Loader=yaml.FullLoader)

    # Parse input files
    if "QARTOD_TESTS_CONFIGURATION_PATH" in config:
        with open(config["QARTOD_TESTS_CONFIGURATION_PATH"], encoding="UTF-8") as f:
            config["qartod_tests_config"] = json.load(f)
    if "HAKAI_TESTS_CONFIGURATION_PATH" in config:
        with open(config["HAKAI_TESTS_CONFIGURATION_PATH"], encoding="UTF-8") as f:
            config["hakai_tests_config"] = json.load(f)
    if "HAKAI_CTD_ATTRIBUTES" in config:
        with open(config["HAKAI_CTD_ATTRIBUTES"], encoding="UTF-8") as f:
            config["netcdf_attributes"] = json.load(f)
    return config


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


def run_qc_profiles(df, config):
    """
    Main method that runs on a number of profiles a series of QARTOD tests and specific to the Hakai CTD Dataset.
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
    df_static = (
        df.query("direction_flag in ('s')")
        .groupby(["hakai_id", "measurement_dt"])
        .progress_apply(
            lambda x: _run_ioosqc_on_dataframe(
                x, qartod_config, **config["ioos_qc_coords_mapping"]
            ),
        )
    )
    # Regroup back together profiles and static data
    df = df_profiles.append(df_static).reset_index(drop=True)

    # HAKAI SPECIFIC TESTS #
    # This section regroup different non QARTOD tests which are specific to
    # Hakai profile dataset. Most of the them
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
    #  Find Profiles that were flagged near the bottom and assume this is
    # likely related to having it the bottom.
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


def qc_profiles(processed_cast_filter=None, config=None):

    # Handle default inputs
    if processed_cast_filter is None:
        logger.info("QC processed data that is not qced yet")
        # QC data that has been processed but not qced yet
        processed_cast_filter = "processing_stage={8_rbr_processed,8_binAvg}&limit=-1"
    if config is None:
        config = read_config_yaml(DEFAULT_CONFIG_PATH)

    # Get the list of hakai_ids to qc
    url = f"{config['HAKAI_API_SERVER_ROOT']}/{config['CTD_CAST_ENDPOINT']}?{processed_cast_filter}"
    client = Client()
    logger.info("Retrieve: %s", url)
    response = client.get(url)
    df_casts = pd.DataFrame(response.json())
    if df_casts.empty:
        logger.info("No Drops needs to be QC")
        return None, None
    logger.info("%s needs to be qc!", len(df_casts))

    qced_cast_data = []
    for chunk in np.array_split(
        df_casts, np.ceil(len(df_casts) / config["CTD_CAST_CHUNKSIZE"])
    ):
        logger.debug("QC hakai_ids: %s", str(chunk["hakai_id"]))
        response_data = client.get(
            "%s/%s?hakai_id={%s}&limit=-1"
            % (
                config["HAKAI_API_SERVER_ROOT"],
                config["CTD_CAST_DATA_ENDPOINT"],
                ",".join(chunk["hakai_id"].values),
            )
        )
        df_qced = pd.DataFrame(response_data.json())
        df_qced = _derived_ocean_variables(df_qced)
        df_qced = run_qc_profiles(df_qced, config)

        # Convert QARTOD to string temporarily
        qartod_columns = df_qced.filter(regex="_flag_level_1").columns
        # TODO Drop once qartod columns are of INT type in hakai DB
        df_qced[qartod_columns] = df_qced[qartod_columns].astype(str)
        df_qced = df_qced.replace({"": None})

        # Update qced casts stage and error log
        chunk["processing_stage"] = "9_qc_auto"
        chunk["process_error"] = chunk["process_error"].fillna("")
        # Upload to server
        if config["UPDATE_SERVER_DATABASE"]:
            for _, row in tqdm(
                chunk.iterrows(), desc="Upload flags", unit="profil", total=len(chunk)
            ):
                logger.debug("Upload qced %s", row["hakai_id"])
                json_string = _generate_process_flags_json(row, df_qced)
                response = client.post(
                    f"{config['HAKAI_API_SERVER_ROOT']}/ctd/process/flags/json/{row['ctd_cast_pk']}",
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


def get_hakai_ctd_stations_list():
    """Return  a dataframe of all the work_area,stations available in the hakai database"""
    client = Client()
    url = f"{config['HAKAI_API_SERVER_ROOT']}/{config['CTD_CAST_ENDPOINT']}?limit=-1&fields=work_area,station,station_latitude,station_longitude&distinct"
    response = client.get(url)
    if response.status_code != 200:
        logger.error(response.text)
        return
    return pd.DataFrame(response.json())


def generate_hakai_provisional_netcdf_dataset(config=None):
    # Get Hakai Station List
    # netcdf_attributes = json.loads(config['']
    if config is None:
        config = read_config_yaml(DEFAULT_CONFIG_PATH)

    station_list = get_hakai_ctd_stations_list()
    station_list = station_list.query(
        f"work_area in ('CALVERT','JOHNSTONE STRAIT','QUADRA') and station not in {config['IGNORED_HAKAI_STATIONS']}"
    ).set_index("station", drop=False)
    for station, row in station_list.iterrows():
        df_data, df_casts = qc_profiles(
            f"limit=-1&station={station}&(status=null|status='')&process_error=null"
        )
        # If no data keep going
        if df_data is None:
            continue

        # Generate file name
        file_name_output = f"./output/HakaiWaterPropertiesInstrumentProfileProvisional/{row['work_area']}/{row['work_area']}_{row['station']}_{df_casts['start_dt'].min()}-{df_casts['start_dt'].max()}.nc"
        subdir = os.path.dirname(file_name_output)
        if not os.path.isdir(subdir):
            logger.info("Generate directory: %s", subdir)
            os.makedirs(subdir)

        # Convert to xarray
        ds = df_data.to_xarray()

        # Add attributes from config
        ds.attrs = config["netcdf_attributes"]["GLOBAL"]
        for var, attrs in config["netcdf_attributes"].items():
            if var in ds:
                ds[var].attrs = attrs

        # Standardize columns and encoding
        standardize_dateset(ds).to_netcdf(file_name_output)


def generate_hakai_ctd_research_dataset(config):
    # Get Hakai Station List
    # netcdf_attributes = json.loads(config['']
    if config is None:
        config = read_config_yaml(DEFAULT_CONFIG_PATH)

    # get qced hakai_id list
    client = Client()
    logger.info("Retrieve list of QC profiles")
    response = client.get(
        f"{config['HAKAI_API_SERVER_ROOT']}/{config['CTD_CAST_QC_ENDPOINT']}?limit=-1"
    )
    if response.status_code != 200:
        raise RuntimeError(response.text)

    df_qc = pd.DataFrame(response.json())
    df_qc.set_index("hakai_id", inplace=True)
    df_qc["depth_flag"].fillna("AV", inplace=True)
    logging.info("%s qced profiles available", len(df_qc))
    for chunk in np.array_split(
        df_qc, np.ceil(len(df_qc) / config["CTD_CAST_CHUNKSIZE"])
    ):
        # TODO Replace by a query to ctd_file_cast_data and ctd_file_cast when flags will be available
        logger.info("Download CTD data and run automated qc")
        df_data, df_casts = qc_profiles(
            "hakai_id={%s}&limit=-1" % ",".join(chunk.index)
        )
        # Output only downcast
        df_data = df_data.query("direction_flag=='d'")
        df_casts = df_casts.set_index(["hakai_id"], drop=False)
        # Drop values that are flagged as SVD
        logger.info("Merge auto qc data to manual qc and discard unqced and bad data")
        df_data_qc = pd.merge(
            df_data, df_qc, on="hakai_id", suffixes=("", "_manual_qc")
        )
        for var in config["RESEARCH_CTD_VARIABLES"]:
            df_data_qc.loc[
                df_data_qc[[var + "_flag_level_1", var + "_flag_manual_qc"]]
                .isin([4, 9, "SVD", "NA", None])
                .any(axis=1),
                var,
            ] = pd.NA

        # Drop flag variables and rows and depth
        ignored_variables = [
            var for var in df_data_qc if var not in config["netcdf_attributes"]
        ]
        df_data_qc = (
            df_data_qc.drop(columns=ignored_variables)
            .dropna(axis=0, how="all", subset=config["RESEARCH_CTD_VARIABLES"])
            .dropna(axis=0, how="all", subset=["depth"])
        )
        logger.info("Save each profiles to a separate NetCDF")
        for hakai_id, df_hakai_id in df_data_qc.groupby(["hakai_id"]):
            # Generate file name
            cast = df_casts.loc[hakai_id]
            file_name_output = (
                "./output/HakaiWaterPropertiesInstrumentProfileResearch/"
                + f"{cast['work_area']}/{cast['station']}/"
                + f"{hakai_id}_Research_downcast.nc"
            )
            subdir = os.path.dirname(file_name_output)
            if not os.path.isdir(subdir):
                logger.info("Generate directory: %s", subdir)
                os.makedirs(subdir)

            # Drop empty variables, index by start_dt and depth and convert to xarray dataset
            ds = (
                df_hakai_id.dropna(axis=1, how="all")
                .set_index(["hakai_id", "depth"])
                .to_xarray()
            )

            # Add cast data as either variable or global attribute
            ds.attrs = config["netcdf_attributes"]["GLOBAL"]
            for key, value in cast.dropna().to_dict().items():
                if key in config["netcdf_attributes"]:
                    if key in ds:
                        continue
                    ds[key] = value
                elif key in ds and key not in config["netcdf_attributes"]:
                    ds = ds.drop(key)
                    ds.attrs[key] = value
                elif type(value) in (bool,):
                    ds.attrs[key] = str(value)
                else:
                    ds.attrs[key] = value

            # Add variable attributes
            for var, attrs in config["netcdf_attributes"].items():
                if var in ds:
                    ds[var].attrs = attrs

            # Standardize columns and encoding
            standardize_dateset(ds).to_netcdf(file_name_output)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--update_provisional", action="store_true")
    parser.add_argument("--update_research", action="store_true")
    parser.add_argument("--qc_profiles", action="store_true")
    parser.add_argument("--qc_profiles_filter", default=None)
    parser.add_argument("--config", default=None)
    args = parser.parse_args()
    # Read default config and update with given one
    config = read_config_yaml(DEFAULT_CONFIG_PATH)
    if args.config:
        config.update(read_config_yaml(args.config))

    # Run Query
    if args.qc_profiles:
        df = qc_profiles(args.processed_cast_filter, config)
    if args.update_provisional:
        generate_hakai_provisional_netcdf_dataset(config)
    if args.update_research:
        generate_hakai_ctd_research_dataset(config)
