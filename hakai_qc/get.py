import pandas as pd
import pkg_resources
from hakai_api import Client
import json
import hakai_qc
import os


def hakai_stations():
    # Load Hakai Station List
    station_list_file = "HakaiStationLocations.csv"
    station_list_path = pkg_resources.resource_filename(__name__, station_list_file)
    hakai_stations_list = pd.read_csv(station_list_path, sep=';', index_col='Station')

    # Index per station name
    hakai_stations_list.index = hakai_stations_list.index.str.upper()  # Force Site Names to be uppercase

    # Convert Depth columns to float values
    hakai_stations_list['Bot_depth'] = pd.to_numeric(hakai_stations_list['Bot_depth'],
                                                     errors='coerce').astype(float)
    hakai_stations_list['Bot_depth_GIS'] = pd.to_numeric(hakai_stations_list['Bot_depth_GIS'],
                                                         errors='coerce').astype(float)

    return hakai_stations_list


def hakai_ctd_data(filterUrl):
    """
    hakai_ctd_data(filterUrl) method used the Hakai Python API Client to query Processed CTD data from the Hakai
    database based on the filter provided. The data is then converted to a Pandas data frame.
    """
    if filterUrl is None:
        print('You don''t want to download all the data!')
    # Get Hakai Data
    # Get Data from Hakai API
    client = Client()  # Follow stdout prompts to get an API token

    # CTD data endpoint
    hakai_ctd_endpoint = 'ctd/views/file/cast/data'

    # Make a data request for sampling stations
    url = '%s/%s?%s' % (client.api_root, hakai_ctd_endpoint, filterUrl)
    response = client.get(url)
    df = pd.DataFrame(response.json())
    return df, url


def json_config(config_file):
    """
    Small tool to parse json configuration file
    """
    with open(os.path.join(hakai_qc.__path__[0], 'config', config_file)) as json_file:
        qc_config = json.load(json_file)

    return qc_config


def config_as_dataframe(qc_config):
    # Convert qc_config dictionary to a dataframe which can be easier to read by the user.
    qc_table = pd.DataFrame()
    for var in qc_config.keys():
        for module in qc_config[var].keys():
            test = pd.DataFrame.from_dict(qc_config[var][module]).unstack()
            test.name = 'Value'
            test = test.reset_index().rename({'level_0': 'Test', 'level_1': 'Input'}, axis=1)
            test['Module'] = module
            test['Variable'] = var
            qc_table = qc_table.append(test)

    qc_table = qc_table.set_index(['Variable', 'Module', 'Test', 'Input']).dropna()
    return qc_table


def hakai_api_selected_variables():
    variables_to_get = ['ctd_file_pk', 'ctd_cast_pk', 'hakai_id', 'ctd_data_pk', 'filename',
                        'device_model', 'device_sn',
                        'work_area', 'cruise', 'station',
                        'cast_number', 'latitude', 'longitude', 'start_dt',
                        'bottom_dt', 'end_dt', 'duration', 'start_depth', 'bottom_depth', 'direction_flag',
                        'measurement_dt', 'descent_rate', 'conductivity', 'conductivity_flag',
                        'temperature', 'temperature_flag', 'depth', 'depth_flag', 'pressure',
                        'pressure_flag', 'par', 'par_flag', 'flc', 'flc_flag', 'turbidity',
                        'turbidity_flag', 'ph', 'ph_flag', 'salinity', 'salinity_flag',
                        'spec_cond', 'spec_cond_flag', 'dissolved_oxygen_ml_l',
                        'dissolved_oxygen_ml_l_flag', 'rinko_do_ml_l', 'rinko_do_ml_l_flag',
                        'dissolved_oxygen_percent', 'dissolved_oxygen_percent_flag',
                        'oxygen_voltage', 'oxygen_voltage_flag', 'c_star_at', 'c_star_at_flag',
                        'sos_un', 'sos_un_flag', 'backscatter_beta', 'backscatter_beta_flag',
                        'cdom_ppb', 'cdom_ppb_flag']
    return variables_to_get


def fail_test_hakai_id():
    hakai_ids = ['080217_2017-01-26T16:56:39.000Z', '080217_2017-01-15T17:57:21.667Z',
                 '080217_2017-01-15T18:06:32.000Z', '080217_2016-11-26T20:23:06.500Z',
                 '080217_2016-11-26T17:24:19.333Z']
    return hakai_ids

