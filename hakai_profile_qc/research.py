"""
hakai_profile_qc.research regroup all the different tools used to generate the NetCDF files associated with the Hakai Research CTD dataset found on ERDDAP.
"""

import pandas as pd
import numpy as np
import xarray as xr
from datetime import datetime as dt
import json

import os

import re
from hakai_api import Client
import warnings
import argparse

import hakai_profile_qc.get
import hakai_profile_qc.transform
import hakai_profile_qc.review


client = Client()  # Follow stdout prompts to get an API token

config_path = os.path.join(os.path.dirname(__file__), "config")


def generate_netcdf(
    hakai_id,
    path_out,
    overwrite=True,
    variable_list=None,
    creator_attributes=None,
    extra_global_attributes=None,
    extra_variable_attributes=None,
    profile_id="hakai_id",
    timeseries_id="station",
    depth_var="depth",
    file_name_append="_Research",
    mandatory_output_variables=("measurement_dt", "direction_flag", "cast_comments"),
    mask_qartod_flag=None,
    level_1_flag_suffix="_qartod_flag",
    level_2_flag_suffix="_flag_description",
    remove_empty_variables=True,
):
    # Load Default attributes
    with open(
        os.path.join(
            config_path,
            "hakai_profile_default_attributes.json",
        )
    ) as f:
        attributes = json.loads(f.read())

    # Define Global attributes
    global_attributes = attributes.pop("GLOBAL")
    if extra_global_attributes:
        for key, value in extra_global_attributes.items():
            global_attributes[key] = value

    global_attributes["title"] = ("Hakai Research CTD Profile: " + hakai_id,)
    global_attributes["date_created"] = (str(dt.utcnow().isoformat()),)
    global_attributes["id"] = hakai_id

    # Look if overwrite=True and output file exist already,
    hakai_id_str = hakai_id.replace(":", "").replace(".", "")  # Output Hakai ID String
    if not overwrite:
        for root, dirs, files in os.walk(path_out):
            if any(re.match(hakai_id_str, file) for file in files):
                print(hakai_id + " already exists")
                # If already exist stop function
                return

    # Create list of variables to use as coordinates
    coordinate_list = [timeseries_id, profile_id, depth_var]

    # Retrieve data to be save
    # TODO TEMPORARY SECTION TO DEAL WITH NO QARTOD FLAGS ON THE DATA BASE
    data = hakai_profile_qc.review.run_tests(hakai_id=[hakai_id], filter_variables=False)
    if data is None:
        print(hakai_id + " no data available")
        return
    data = data[data["direction_flag"] == "d"]  # Keep downcast only
    data_meta = hakai_profile_qc.get.table_metadata_info("hakai_id=" + hakai_id)
    # # TODO FOLLOWING SECTION SHOULD BE USED IN THE FUTURE WHEN DATABASE IS GOOD TO GO
    # data, data_meta = _get_hakai_ctd_full_data(endpoint_list['ctd_data'],
    #                                            'hakai_id=' + hakai_id + '&direction_flag=d&limit=-1',
    #                                            get_columns_info=True)
    # ####
    # Cast Related information
    cast = hakai_profile_qc.get.hakai_ctd_data(
        f"hakai_id={hakai_id}&limit=-1", "ctd/views/file/cast"
    )

    # Get Station info
    # TODO We'll get it from the CSV file since I can't retrieve it yet from the Hakai DataBase
    stations = hakai_profile_qc.get.hakai_stations()
    if cast["station"][0] in stations.index:
        cast["longitude_station"], cast["latitude_station"] = stations.loc[
            cast["station"]
        ][["longitude", "latitude"]].values[0]
    else:
        cast["longitude_station"], cast["latitude_station"] = cast[
            ["longitude", "latitude"]
        ].values[0]

    # Review there's actually any good data from QARTOD
    if all(data[depth_var + level_1_flag_suffix].isin([3, 4])):
        warnings.warn("No real good data is associated to " + hakai_id, RuntimeWarning)
        return

    # Make some data conversion to compatible with ERDDAP NetCDF Format
    def _convert_dt_columns(df):
        time_var_list = df.dropna(axis=1, how="all").filter(regex="_dt$|time_").columns
        df[time_var_list] = df[time_var_list].apply(lambda x: pd.to_datetime(x))
        return df

    # Convert time variables to datetime objects
    data = _convert_dt_columns(data)
    cast = _convert_dt_columns(cast)

    # Sort vertical variables and profile specific variables
    profile_variables = set(data.columns).intersection(set(cast.columns)) - set(
        coordinate_list
    )
    vertical_variables = set(data.columns) - profile_variables - set(coordinate_list)
    extra_variables = set(cast.columns) - set(profile_variables) - set(coordinate_list)

    # Filter Vertical Variables to just the accepted ones
    if variable_list is not None:
        vertical_variables = set(
            var
            for var in vertical_variables
            if re.match("^" + "|^".join(variable_list), var)
        ).union(mandatory_output_variables)

    # Mask Records associated with Rejected QARTOD Flags
    if type(mask_qartod_flag) is list:
        for Q_col in data[vertical_variables].filter(like=level_1_flag_suffix).columns:
            var_col = Q_col.replace(level_1_flag_suffix, "")
            # Replace value by NaN if flag rejected
            data.loc[data[Q_col].isin(mask_qartod_flag), var_col] = np.NaN
            # Drop Level 2 flag if data is rejected
            data.loc[
                data[Q_col].isin(mask_qartod_flag), var_col + level_2_flag_suffix
            ] = ""

    # Define Variable Attributes Dictionaries
    # From database metadata
    map_hakai_database = {"display_column": "long_name", "variable_units": "units"}
    database_attributes = (
        data_meta.loc[["display_column", "variable_units"]]
        .dropna(axis=1)
        .rename(map_hakai_database, axis="index")
        .to_dict()
    )

    # Hakai QC has priority over database metadata
    variable_attributes = {}
    variable_attributes.update(database_attributes)
    variable_attributes.update(attributes)
    if extra_variable_attributes:
        variable_attributes.update(extra_variable_attributes)

    # Remove empty variables
    if remove_empty_variables:
        vertical_variables = data[vertical_variables].dropna(axis=1, how="all").columns
        cast_variables = (
            cast[extra_variables.union(profile_variables)]
            .dropna(axis=1, how="all")
            .columns
        )
    else:
        cast_variables = extra_variables.union(profile_variables)

    # Create Xarray DataArray for each types and merge them together after
    ds_vertical = hakai_profile_qc.transform.dataframe_to_erddap_xarray(
        data.set_index(coordinate_list)[vertical_variables],
        profile_id=profile_id,
        timeseries_id=timeseries_id,
        variable_attributes=variable_attributes,
        flag_columns={"_qartod_flag$": ["QARTOD", "aggregate_quality_flag"]},
    )
    ds_profile = hakai_profile_qc.transform.dataframe_to_erddap_xarray(
        cast.set_index([timeseries_id, profile_id])[cast_variables],
        profile_id=profile_id,
        timeseries_id=timeseries_id,
        variable_attributes=variable_attributes,
        flag_columns={"_qartod_flag$": ["QARTOD", "aggregate_quality_flag"]},
    )

    # Merge the profile_id and vertical data combine both attrs with profile overwriting the vertical ones.
    ds = xr.merge([ds_profile, ds_vertical], join="outer")
    ds.attrs = ds_vertical.attrs
    ds.attrs.update(ds_profile.attrs)

    # Add Global attribute documentation
    ds.attrs["comments"] = str(cast["comments"][0])
    ds.attrs["processing_level"] = str(cast["processing_stage"][0])
    ds.attrs["history"] = str(
        {
            "vendor_metadata": str(cast["vendor_metadata"][0]),
            "processing_log": str(cast["process_log"][0]),
        }
    )
    ds.attrs["instrument"] = str(
        cast["device_model"][0]
        + " SN"
        + cast["device_sn"][0]
        + " Firmware"
        + cast["device_firmware"][0]
    )
    ds.attrs["work_area"] = str(cast["work_area"][0])
    ds.attrs["station"] = str(cast["station"][0])

    # Add user defined and variable specific global attributes
    ds.attrs.update(global_attributes)
    if creator_attributes is not None:
        ds.attrs.update(creator_attributes)
    if extra_global_attributes is not None:
        ds.attrs.update(extra_global_attributes)

    # Define output path and file
    if (ds["direction_flag"] == b"d").all():  # If all downcast
        file_name_append = file_name_append + "_downcast"
    if (ds["direction_flag"] == b"u").all():  # If all downcast
        file_name_append = file_name_append + "_upcast"

    # Sort variable order based on the order from the hakai data table followed by the cast table
    cast_list = cast.columns
    data_var_list = data.columns
    cast_var_order = list(cast_list[cast_list.isin(list(ds.keys()))])
    data_var_order = list(data_var_list[data_var_list.isin(list(ds.keys()))])
    var_list = list(dict.fromkeys(cast_var_order + data_var_order))

    # Save to NetCDF in a subdirectory 'area/station/' and in the original order given by the database
    path_out_sub_dir = os.path.join(
        path_out, ds.attrs["work_area"], ds.attrs["station"]
    )
    if not os.path.exists(path_out_sub_dir):
        os.makedirs(path_out_sub_dir)
    file_output_path = os.path.join(
        path_out_sub_dir, hakai_id_str + file_name_append + ".nc"
    )
    ds[var_list].to_netcdf(file_output_path)
    return


def update_research_dataset_with_ctd_log(
    path_out=r"", creator_name=None, overwrite=True
):
    """
    Method use to combined the manually QCed results from the ctd_qc table on the Hakai Database and the automatically generated flags.
    """
    ctd_qc_log_endpoint = "eims/views/output/ctd_qc"
    df_qc = hakai_profile_qc.get.hakai_ctd_data(
        "limit=-1", endpoint=ctd_qc_log_endpoint
    )

    # Filter QC log by keeping only the lines that have inputs
    df_qc = df_qc.loc[df_qc.filter(like="_flag").notna().any(axis=1)].copy()
    df_qc = df_qc.set_index("hakai_id")

    # Review in output directory files already available
    if not overwrite:
        existing_files = []
        for (dirpath, dirnames, filenames) in os.walk(path_out):
            existing_files.extend(filenames)

        # Review which files already exist and remove them from the list
        remove_id = df_qc.index.str.replace(r"[\:\.]", "", regex=True).isin(
            pd.Series(existing_files).str.replace("_Research.*", "")
        )
        df_qc = df_qc.loc[~remove_id]

    # Generate NetCDFs
    for hakai_id, row in df_qc.iterrows():
        # Retrieve flag columns that starts with AV, drop trailing _flag
        var_to_save = row.filter(like="_flag").str.startswith("AV").dropna()
        var_to_save = var_to_save[var_to_save].rename(
            index=lambda x: re.sub("_flag$", "", x)
        )

        if len(var_to_save.index) > 0:
            print("Save " + hakai_id)
            # TODO add a step to identify reviewer and add reviewer review to history
            # TODO add an input to add a creator attribute.
            # TODO should we overwrite already existing files overwritten
            #  there's an option to overwrite or not now but we may want to add an method to just append new variables
            # with warnings.catch_warnings():
            #     warnings.filterwarnings('error')
            try:
                generate_netcdf(
                    hakai_id,
                    path_out,
                    variable_list=var_to_save.index.tolist(),
                    mask_qartod_flag=[2, 3, 4, 9],
                    overwrite=overwrite,
                )
            except RuntimeWarning:
                print("Hakai ID " + hakai_id + " failed")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-hakai_id")
    parser.add_argument("-path")
    args = parser.parse_args()
    if args.path:
        path = args.path
    else:
        path = ""
    for hakai_id in args.hakai_id.split(","):
        generate_netcdf(hakai_id, path)
