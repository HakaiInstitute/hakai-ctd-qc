import pandas as pd
import xarray as xr
from datetime import datetime as dt
import json

import os
import pkg_resources

from hakai_api import Client
import hakai_qc


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


def research_profile_netcdf(hakai_id,
                            path_out,
                            creator_attributes={},
                            extra_global_attributes={},
                            extra_variable_attributes={},
                            profile_id='hakai_id',
                            timeseries_id='station',
                            depth_var='depth',
                            file_name_append='_Research'
                            ):

    # Define the general global attributes associated to all hakai profile data and the more specific ones associated
    # with each profile and data submission.
    general_global_attributes = {
        'institution': 'Hakai Institute',
        'project': 'Hakai Oceanography',
        'summary': 'text describing that specific data',
        'comment': '',
        'infoUrl': 'hakai.org',
        'keywords': "conductivity,temperature,salinity,depth,pressure,dissolved oxygen",
        'acknowledgment': 'Hakai Field Techniciens, research and IT groups',
        'naming_authority': 'Hakai Instititute',
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

    # Retrieve data for the specified hakai_id
    # Let's define the endpoints we'll use to get the data from the Hakai Database
    endpoint_list = {'ctd_data': 'ctd/views/file/cast/data',
                     'metadata': 'ctd/views/file/cast'}

    # Create list of variables to use as coordindates
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
    data, data_meta = _get_hakai_ctd_full_data(endpoint_list['ctd_data'],
                                                'hakai_id=' + hakai_id + '&direction_flag=d&limit=-1',
                                               get_columns_info=True)
    cast, cast_meta = _get_hakai_ctd_full_data(endpoint_list['metadata'],
                                                'hakai_id=' + hakai_id)

    # Make some data conversion to compatible with ERDDAP NetCDF Format
    def convert_dt_columns(df):
        time_var_list = df.dropna(axis=1, how='all').filter(regex='_dt$|time_').columns
        for var in time_var_list:
            df[var] = pd.to_datetime(df[var], utc=True)
        return df

    # Convert time variables to datetime objects
    data = convert_dt_columns(data)
    cast = convert_dt_columns(cast)

    # Sort vertical variables and profile specific variables
    profile_variables = set(data.columns).intersection(set(cast.columns)) - set(coordinate_list)
    vertical_variables = set(data.columns) - profile_variables - set(coordinate_list)
    extra_variables = set(cast.columns) - set(profile_variables) - set(coordinate_list)

    # Create Xarray DataArray for each types and merge them together after
    ds_vertical = hakai_qc.transform.dataframe_to_erddap_xarray(
        data.set_index(coordinate_list)[vertical_variables].dropna(axis=1, how='all'),
        profile_id=profile_id, timeseries_id=timeseries_id)
    ds_profile = hakai_qc.transform.dataframe_to_erddap_xarray(
        cast.set_index([timeseries_id, profile_id])[extra_variables.union(profile_variables)].dropna(axis=1, how='all'),
        profile_id=profile_id, timeseries_id=timeseries_id)

    # Merge the profile_id and vertical data combine both attrs with profile overwriting the vertical ones.
    ds = xr.merge([ds_profile, ds_vertical], join='outer')
    ds.attrs = ds_vertical.attrs
    ds.attrs.update(ds_profile.attrs)

    # Add Global attribute documentation
    ds.attrs['comments'] = str(cast['comments'][0])
    ds.attrs['processing_level'] = str(cast['processing_stage'][0])
    ds.attrs['history'] = str({'vendor_metadata': str(cast['vendor_metadata'][0]),
                               'processing_log': str(cast['process_log'][0])})
    ds.attrs['instrument'] = str(cast['device_model'][0]+
                                 ' SN'+cast['device_sn'][0]+
                                 ' Firmware'+cast['device_firmware'][0])

    # Add user defined and variable specific global attributes
    ds.attrs.update(general_global_attributes)
    ds.attrs.update(specific_global_attribute)
    ds.attrs.update(creator_attributes)
    ds.attrs.update(extra_global_attributes)

    # Add Hakai Database variable attributes
    map_hakai_database = {'display_column': 'long_name', 'variable_units': 'units'}
    not_empty_var = data_meta.loc[['display_column', 'variable_units']].dropna(axis=1)
    for var in not_empty_var:
        if var in ds:
            for key in not_empty_var.index.values:
                ds[var].attrs[map_hakai_database[key]] = not_empty_var[var][key]

    # Add provided variable attributes
    for var in extra_variable_attributes:
        ds[var].attrs.update(extra_variable_attributes[var])

    # Define output path and file
    if (ds['direction_flag']==b'd').all():  # If all downcast
        file_name_append = file_name_append + '_downcast'
    if (ds['direction_flag'] == b'u').all():  # If all downcast
        file_name_append = file_name_append + '_upcast'
    hakai_id_str = hakai_id.replace(':', '').replace('.', '')

    file_output_path = os.path.join(path_out, hakai_id_str + file_name_append + '.nc')

    # Save to NetCDF
    ds.to_netcdf(file_output_path)
    return
