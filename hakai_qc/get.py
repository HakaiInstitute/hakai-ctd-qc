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


import folium


def flag_result_map(df,
                    flag_variable='position_qartod_flag',
                    groupby_var='hakai_id'):
    # Start the map with center on the average lat/long
    center_map = df.groupby(groupby_var)[['latitude', 'longitude']].mean().mean().to_list()

    # Start by defining the map
    m = folium.Map(
        location=center_map,
        zoom_start=9, control_scale=True,
        tiles='Stamen Terrain')
    # Create groups
    fg = folium.FeatureGroup('QARTOD FLAG')

    # Add each flagged profiles grouped by group variable (default: hakai_id) as a separate icon on the map
    # SUSPECT PROFILES
    f3 = folium.plugins.FeatureGroupSubGroup(fg, 'SUSPECT')
    for index, row in df[df[flag_variable] == 3].groupby(by=groupby_var):
        f3.add_child(
            folium.Marker(row[['latitude', 'longitude']].mean().tolist(), popup='[SUSPECT] hakai_id: ' + str(index),
                          icon=folium.Icon(color='orange', icon='question-sign')))
    # FAIL PROFILES
    f4 = folium.plugins.FeatureGroupSubGroup(fg, 'FAIL')
    for index, row in df[df[flag_variable] == 4].groupby(by=groupby_var):
        f4.add_child(
            folium.Marker(row[['latitude', 'longitude']].mean().tolist(), popup='[FAIL] hakai_id: ' + str(index),
                          icon=folium.Icon(color='red', icon='question-sign')))
    # UNKNOWN
    f9 = folium.plugins.FeatureGroupSubGroup(fg, 'UNKNOWN')
    for index, row in df[df[flag_variable] == 9].groupby(by=groupby_var):
        f9.add_child(
            folium.Marker(row[['latitude', 'longitude']].mean().tolist(), popup='[UNKNOWN] hakai_id: ' + str(index),
                          icon=folium.Icon(color='purple', icon='question-sign')))

    # All the ones that succeed can just be a fast marker cluster
    f1 = folium.plugins.FeatureGroupSubGroup(fg, 'GOOD')
    f1.add_child(folium.plugins.FastMarkerCluster(df[df[flag_variable] == 1]
                                                  .groupby(by=groupby_var).first()
                                                  [['latitude', 'longitude']].values))

    m.add_child(fg)
    m.add_child(f1)
    m.add_child(f3)
    m.add_child(f4)
    m.add_child(f9)
    folium.LayerControl().add_to(m)
    return m


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

