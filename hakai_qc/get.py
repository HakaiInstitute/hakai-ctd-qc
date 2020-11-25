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

