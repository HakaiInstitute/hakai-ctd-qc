import pandas as pd
import pkg_resources
from hakai_api import Client
import json
import hakai_qc
import os

import matplotlib.pyplot as plt
import matplotlib.lines as mlines
import seaborn as sns

import folium
import folium.folium as Map
import folium.plugins as plugins


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


def flag_result_plot(df,
                      variables_to_plot,
                      hakai_id_to_plot,
                      y_axis_var='depth',
                      flag_type='_qartod_aggregate'):

    # Palette color for flag
    color_dict = {1: 'seagreen', 2: 'gray', 3: 'darkorange', 4: 'red', 9: 'purple'}

    # Define legend
    legend_elements = [
        mlines.Line2D([], [], color=color_dict[1], marker='s', markersize=10, linestyle='None', label='GOOD'),
        mlines.Line2D([], [], color=color_dict[3], marker='s', markersize=10, linestyle='None', label='SUSPECT'),
        mlines.Line2D([], [], color=color_dict[4], marker='s', markersize=10, linestyle='None', label='BAD'),
        mlines.Line2D([], [], color='black', marker='.', markersize=10, linestyle='None', label='Down Cast'),
        mlines.Line2D([], [], color='black', marker='x', markersize=7, linestyle='None', label='Up Cast')]

    # Loop  through each profiles and variable and create plot
    for hakai_id in hakai_id_to_plot:
        print(hakai_id)
        plt.figure()
        fig, axs = plt.subplots(1, len(variables_to_plot),
                                sharex=False, sharey=True)
        fig.set_figwidth(4 * len(variables_to_plot))
        fig.set_figheight(10)
        fig.suptitle('Hakai ID: ' + hakai_id)

        axs[0].invert_yaxis()

        kk = 0
        for variable in variables_to_plot:
            g = sns.scatterplot(data=df[df['hakai_id'] == hakai_id],
                                x=variable, y=y_axis_var,
                                hue=variable + flag_type,
                                palette=color_dict,
                                style='direction_flag',
                                linewidth=0, ax=axs[kk], legend=False)
            kk = kk + 1
        plt.subplots_adjust(wspace=0, hspace=0)

        plt.legend(handles=legend_elements,
                   bbox_to_anchor=(1, 1.02),
                   loc='lower right', ncol=2, borderaxespad=0.)


def flag_result_map(df,
                    flag_variable='position_qartod_aggregate',
                    groupby_var='hakai_id'):
    # Start the map with center on the average lat/long
    center_map = df.groupby(groupby_var)[['latitude', 'longitude']].mean().mean().to_list()

    # Start by defining the map
    m = folium.Map(
        location=center_map,
        zoom_start=10, control_scale=True,
        tiles='Stamen Terrain')

    # Add each flagged profiles as a separate icon on the map
    for index, row in df[df[flag_variable] == 3].groupby(by=groupby_var):
        folium.Marker(row[['latitude', 'longitude']].mean().tolist(), popup='[SUSPECT] hakai_id: ' + str(index),
                      icon=folium.Icon(color='orange')).add_to(m)
    for index, row in df[df[flag_variable] == 4].groupby(by=groupby_var):
        folium.Marker(row[['latitude', 'longitude']].mean().tolist(), popup='[FAIL] hakai_id: ' + str(index),
                      icon=folium.Icon(color='red', icon='fail-sign')).add_to(m)
    for index, row in df[df[flag_variable] == 9].groupby(by=groupby_var):
        folium.Marker(row[['latitude', 'longitude']].mean().tolist(), popup='[UNKNOWN] hakai_id: ' + str(index),
                      icon=folium.Icon(color='purple', icon='question-sign')).add_to(m)

    # All the ones that succeed can just be a fast marker cluster
    plugins.FastMarkerCluster(df.groupby(by=groupby_var).first()[['latitude', 'longitude']].values).add_to(m)

    return m


