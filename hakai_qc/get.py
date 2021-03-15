import pandas as pd
import numpy as np
import xarray as xr
from datetime import datetime as dt
import json

import os
import pkg_resources
import re
from hakai_api import Client
import hakai_qc
import warnings


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


def hakai_ctd_data(filter_url,
                   endpoint='ctd/views/file/cast/data',
                   output_format='dataframe',
                   get_columns_info=False):
    """
    hakai_ctd_data(filterUrl) method used the Hakai Python API Client to query Processed CTD data from the Hakai
    database based on the filter provided. The data is then converted to a Pandas data frame.
    """
    if filter_url is None:
        print('You don''t want to download all the data!')

    # Get Hakai Data
    # Get Data from Hakai API
    client = Client()  # Follow stdout prompts to get an API token

    # Make a data request for sampling stations
    url = '%s/%s?%s' % (client.api_root, endpoint, filter_url)
    response = client.get(url)

    # Sort the different possible inputs
    if output_format == 'dataframe':
        output = pd.DataFrame(response.json())
    elif output_format == 'json':
        output = response.json()
    else:
        output = response

    if get_columns_info:
        url = url + '&meta'
        response = client.get(url)
        columns_info = pd.DataFrame(response.json())
    else:
        columns_info = []

    return output, url, columns_info


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


def research_profile_netcdf(hakai_id,
                            path_out,
                            overwrite=True,
                            variable_list=None,
                            creator_attributes=None,
                            extra_global_attributes=None,
                            extra_variable_attributes=None,
                            profile_id='hakai_id',
                            timeseries_id='station',
                            depth_var='depth',
                            file_name_append='_Research',
                            mandatory_output_variables=('measurement_dt', 'direction_flag', 'cast_comments'),
                            mask_qartod_flag=None,
                            level_1_flag_suffix='_qartod_flag',
                            level_2_flag_suffix='_flag_description',
                            remove_empty_variables=True):
    # Define the general global attributes associated to all hakai profile data and the more specific ones associated
    # with each profile and data submission.
    general_global_attributes = {
        'institution': 'Hakai Institute',
        'project': 'Hakai Oceanography',
        'summary': 'text describing that specific data',
        'comment': '',
        'infoUrl': 'hakai.org',
        'keywords': "conductivity,temperature,salinity,depth,pressure,dissolved oxygen",
        'acknowledgment': 'Hakai Field Technicians, research and IT groups',
        'naming_authority': 'Hakai Institute',
        'standard_name_vocabulary': 'CF 1.3',
        'license': 'unknown',
        'geospatial_lat_units': 'degrees_north',
        'geospatial_lon_units': 'degrees_east'
    }

    specific_global_attribute = {
        'title': 'Hakai Research CTD Profile: ' + hakai_id,
        'date_created': str(dt.utcnow().isoformat()),
        'id': hakai_id,
    }
    # Look if overwrite=True and output file exist already,
    hakai_id_str = hakai_id.replace(':', '').replace('.', '')  # Output Hakai ID String
    if not overwrite:
        for root, dirs, files in os.walk(path_out):
            if any(re.match(hakai_id_str, file) for file in files):
                print(hakai_id + " already exists")
                # If already exist stop function
                return

    # Retrieve data for the specified hakai_id
    # Let's define the endpoints we'll use to get the data from the Hakai Database
    endpoint_list = {'ctd_data': 'ctd/views/file/cast/data',
                     'metadata': 'ctd/views/file/cast'}

    # Create list of variables to use as coordinates
    coordinate_list = [timeseries_id, profile_id, depth_var]

    def _get_hakai_ctd_full_data(endpoint, filterUrl, get_columns_info=False):
        # Get Oauth
        client = Client()
        # Make a data request for sampling stations
        url = '%s/%s?%s' % (client.api_root, endpoint, filterUrl)
        response = client.get(url)
        df = pd.DataFrame(response.json())

        if get_columns_info:
            url = url + '&meta'
            response = client.get(url)
            columns_info = pd.DataFrame(response.json())
        else:
            columns_info = []

        return df, columns_info

    # Retrieve data to be save
    # TODO TEMPORARY SECTION TO DEAL WITH NO QARTOD FLAGS ON THE DATA BASE
    data, data_meta = hakai_qc.run.update_hakai_ctd_profile_data(hakai_id=hakai_id,
                                                                 output='dataframe',
                                                                 filter_variables=False,
                                                                 output_meta=True)
    if data is None:
        print(hakai_id + ' no data available')
        return
    data = data[data['direction_flag'] == 'd']  # Keep downcast only
    # # TODO FOLLOWING SECTION SHOULD BE USED IN THE FUTURE WHEN DATABASE IS GOOD TO GO
    # data, data_meta = _get_hakai_ctd_full_data(endpoint_list['ctd_data'],
    #                                            'hakai_id=' + hakai_id + '&direction_flag=d&limit=-1',
    #                                            get_columns_info=True)
    # ####
    cast, url, cast_meta = hakai_qc.get.hakai_ctd_data('hakai_id=' + hakai_id + '&limit=-1',
                                                       endpoint_list['metadata'])

    # Get Station info
    # TODO We'll get it from the CSV file since I can't retrieve it yet from the Hakai DataBase
    stations = hakai_qc.get.hakai_stations()
    cast['longitude_station'], cast['latitude_station'] = stations.loc[cast['station']][['Long_DD', 'Lat_DD']].values[0]

    # Review there's actually any good data from QARTOD
    if all(data[depth_var + level_1_flag_suffix].isin([3, 4])):
        warnings.warn('No real good data is associated to ' + hakai_id, RuntimeWarning)
        return

    # Make some data conversion to compatible with ERDDAP NetCDF Format
    def _convert_dt_columns(df):
        time_var_list = df.dropna(axis=1, how='all').filter(regex='_dt$|time_').columns
        for var in time_var_list:
            df[var] = pd.to_datetime(df[var], utc=True)
        return df

    # Convert time variables to datetime objects
    data = _convert_dt_columns(data)
    cast = _convert_dt_columns(cast)

    # Sort vertical variables and profile specific variables
    profile_variables = set(data.columns).intersection(set(cast.columns)) - set(coordinate_list)
    vertical_variables = set(data.columns) - profile_variables - set(coordinate_list)
    extra_variables = set(cast.columns) - set(profile_variables) - set(coordinate_list)

    # Filter Vertical Variables to just the accepted ones
    if variable_list is not None:
        vertical_variables = set(var for var in vertical_variables if re.match('^' + '|^'.join(variable_list), var)) \
            .union(mandatory_output_variables)

    # Mask Records associated with Rejected QARTOD Flags
    if type(mask_qartod_flag) is list:
        for Q_col in data[vertical_variables].filter(like=level_1_flag_suffix).columns:
            var_col = Q_col.replace(level_1_flag_suffix, '')
            # Replace value by NaN if flag rejected
            data.loc[data[Q_col].isin(mask_qartod_flag), var_col] = np.NaN
            # Drop Level 2 flag if data is rejected
            data.loc[data[Q_col].isin(mask_qartod_flag), var_col + level_2_flag_suffix] = ''

    # Define Variable Attributes Dictionaries
    # From database metadata
    map_hakai_database = {'display_column': 'long_name', 'variable_units': 'units'}
    database_attributes = data_meta.loc[['display_column', 'variable_units']] \
        .dropna(axis=1).rename(map_hakai_database, axis='index').to_dict()
    # From hakai-qc json
    hakai_qc_attributes = json_config('hakai_profile_variable_attributes.json')

    # Hakai QC has priority over database metadata
    variable_attributes = {}
    variable_attributes.update(database_attributes)
    variable_attributes.update(hakai_qc_attributes)
    if extra_variable_attributes:
        variable_attributes.update(extra_variable_attributes)

    # Remove empty variables
    if remove_empty_variables:
        vertical_variables = data[vertical_variables].dropna(axis=1, how='all').columns
        cast_variables = cast[extra_variables.union(profile_variables)].dropna(axis=1, how='all').columns
    else:
        cast_variables = extra_variables.union(profile_variables)

    # Create Xarray DataArray for each types and merge them together after
    ds_vertical = hakai_qc.transform.dataframe_to_erddap_xarray(
        data.set_index(coordinate_list)[vertical_variables],
        profile_id=profile_id, timeseries_id=timeseries_id, variable_attributes=variable_attributes,
        flag_columns={'_qartod_flag$': ['QARTOD', 'aggregate_quality_flag']})
    ds_profile = hakai_qc.transform.dataframe_to_erddap_xarray(
        cast.set_index([timeseries_id, profile_id])[cast_variables],
        profile_id=profile_id, timeseries_id=timeseries_id, variable_attributes=variable_attributes,
        flag_columns={'_qartod_flag$': ['QARTOD', 'aggregate_quality_flag']})

    # Merge the profile_id and vertical data combine both attrs with profile overwriting the vertical ones.
    ds = xr.merge([ds_profile, ds_vertical], join='outer')
    ds.attrs = ds_vertical.attrs
    ds.attrs.update(ds_profile.attrs)

    # Add Global attribute documentation
    ds.attrs['comments'] = str(cast['comments'][0])
    ds.attrs['processing_level'] = str(cast['processing_stage'][0])
    ds.attrs['history'] = str({'vendor_metadata': str(cast['vendor_metadata'][0]),
                               'processing_log': str(cast['process_log'][0])})
    ds.attrs['instrument'] = str(cast['device_model'][0] +
                                 ' SN' + cast['device_sn'][0] +
                                 ' Firmware' + cast['device_firmware'][0])
    ds.attrs['work_area'] = str(cast['work_area'][0])
    ds.attrs['station'] = str(cast['station'][0])

    # Add user defined and variable specific global attributes
    ds.attrs.update(general_global_attributes)
    ds.attrs.update(specific_global_attribute)
    if creator_attributes is not None:
        ds.attrs.update(creator_attributes)
    if extra_global_attributes is not None:
        ds.attrs.update(extra_global_attributes)

    # Define output path and file
    if (ds['direction_flag'] == b'd').all():  # If all downcast
        file_name_append = file_name_append + '_downcast'
    if (ds['direction_flag'] == b'u').all():  # If all downcast
        file_name_append = file_name_append + '_upcast'

    # Sort variable order based on the order from the hakai data table followed by the cast table
    cast_list = cast.columns
    data_var_list = data.columns
    cast_var_order = list(cast_list[cast_list.isin(list(ds.keys()))])
    data_var_order = list(data_var_list[data_var_list.isin(list(ds.keys()))])
    var_list = list(dict.fromkeys(cast_var_order + data_var_order))

    # Save to NetCDF in a subdirectory 'area/station/' and in the original order given by the database
    path_out_sub_dir = os.path.join(path_out, ds.attrs['work_area'], ds.attrs['station'])
    if not os.path.exists(path_out_sub_dir):
        os.makedirs(path_out_sub_dir)
    file_output_path = os.path.join(path_out_sub_dir, hakai_id_str + file_name_append + '.nc')
    ds[var_list].to_netcdf(file_output_path)
    return
