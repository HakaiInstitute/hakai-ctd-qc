"""
Hakai_profile_qc.get regroup all the different tools used to retrieve hakai specific data and information through the hakai-api.
"""
import pandas as pd
from datetime import datetime as dt

import pkg_resources

from hakai_api import Client
import hakai_profile_qc

client = Client()  # Follow stdout prompts to get an API token


def hakai_stations(source="api"):
    # Load Hakai Station List
    if source == "csv":
        station_list_file = "HakaiStationLocations.csv"
        station_list_path = pkg_resources.resource_filename(__name__, station_list_file)
        hakai_stations_list = pd.read_csv(
            station_list_path, sep=";", index_col="Station"
        )

        # Index per station name
        hakai_stations_list.index = (
            hakai_stations_list.index.str.upper()
        )  # Force Site Names to be uppercase

        # Convert Depth columns to float values
        hakai_stations_list["Bot_depth"] = pd.to_numeric(
            hakai_stations_list["Bot_depth"], errors="coerce"
        ).astype(float)
        hakai_stations_list["Bot_depth_GIS"] = pd.to_numeric(
            hakai_stations_list["Bot_depth_GIS"], errors="coerce"
        ).astype(float)
        hakai_stations_list = hakai_stations_list.rename(
            columns={"Lat_DD": "latitude", "Long_DD": "longitude"}
        )
        hakai_stations_list["depth"] = hakai_stations_list["Bot_depth"].fillna(
            hakai_stations_list["Bot_depth_GIS"]
        )
    elif source == "api":
        # Get Hakai Station list
        hakai_stations_list = hakai_profile_qc.get.hakai_ctd_data(
            "limit=-1", endpoint="eims/views/output_sites"
        )
        # Convert retrieve lat/long from WKB column
        hakai_stations_list = hakai_stations_list.set_index("name")

        # Ignore the stations with no lat/long
        hakai_stations_list.dropna(
            how="all", subset=["latitude", "longitude"], inplace=True
        )
    return hakai_stations_list


def hakai_ctd_data(filter_url, endpoint="ctd/views/file/cast/data", api_root=None):
    """
    hakai_ctd_data(filterUrl) method used the Hakai Python API Client to query Processed CTD data from the Hakai
    database based on the filter provided. The data is then converted to a Pandas data frame.
    """
    if api_root is None:
        api_root = client.api_root

    # Make a data request for sampling stations
    url = "%s/%s?%s" % (api_root, endpoint, filter_url)
    print(f"Retrieve data from: {url}")
    response = client.get(url)

    return pd.DataFrame(response.json())


def table_metadata_info(filter_url, endpoint="ctd/views/file/cast/data"):
    # Get Hakai Data

    # Make a data request for sampling stations
    url = "%s/%s?%s" % (client.api_root, endpoint, filter_url)
    response = client.get(url)

    # Get Columns Metadata Info
    response = client.get(url + "&meta")
    return pd.DataFrame(response.json())


hakai_ctd_data_table_selected_variables = [
    "ctd_file_pk" ,
    "ctd_cast_pk" ,
    "hakai_id" ,
    "ctd_data_pk" ,
    "filename" ,
    "device_model" ,
    "device_sn" ,
    "device_firmware" ,
    "file_processing_stage" ,
    "work_area" ,
    "cruise" ,
    "station" ,
    "cast_number" ,
    "station_longitude" ,
    "station_latitude" ,
    "distance_from_station" ,
    "latitude" ,
    "longitude" ,
    "location_flag" ,
    "location_flag_level_1" ,
    "process_flag" ,
    "process_flag_level_1" ,
    "start_dt" ,
    "bottom_dt" ,
    "end_dt" ,
    "duration" ,
    "start_depth" ,
    "bottom_depth" ,
    "target_depth" ,
    "drop_speed" ,
    "vessel" ,
    "direction_flag" ,
    "measurement_dt" ,
    "descent_rate" ,
    "conductivity" ,
    "conductivity_flag" ,
    "conductivity_flag_level_1" ,
    "temperature" ,
    "temperature_flag" ,
    "temperature_flag_level_1" ,
    "depth" ,
    "depth_flag" ,
    "depth_flag_level_1" ,
    "pressure" ,
    "pressure_flag" ,
    "pressure_flag_level_1" ,
    "par" ,
    "par_flag" ,
    "par_flag_level_1" ,
    "flc" ,
    "flc_flag" ,
    "flc_flag_level_1" ,
    "turbidity" ,
    "turbidity_flag" ,
    "turbidity_flag_level_1" ,
    "ph" ,
    "ph_flag" ,
    "ph_flag_level_1" ,
    "salinity" ,
    "salinity_flag" ,
    "salinity_flag_level_1" ,
    "spec_cond" ,
    "spec_cond_flag" ,
    "spec_cond_flag_level_1" ,
    "dissolved_oxygen_ml_l" ,
    "dissolved_oxygen_ml_l_flag" ,
    "dissolved_oxygen_ml_l_flag_level_1" ,
    "rinko_do_ml_l" ,
    "rinko_do_ml_l_flag" ,
    "rinko_do_ml_l_flag_level_1" ,
    "dissolved_oxygen_percent" ,
    "dissolved_oxygen_percent_flag" ,
    "dissolved_oxygen_percent_flag_level_1" ,
    "oxygen_voltage" ,
    "oxygen_voltage_flag" ,
    "oxygen_voltage_flag_level_1" ,
    "c_star_at" ,
    "c_star_at_flag" ,
    "c_star_at_flag_level_1" ,
    "sos_un" ,
    "sos_un_flag" ,
    "sos_un_flag_level_1" ,
    "backscatter_beta" ,
    "backscatter_beta_flag" ,
    "backscatter_beta_flag_level_1" ,
    "cdom_ppb" ,
    "cdom_ppb_flag" ,
    "cdom_ppb_flag_level_1" 
]