import gsw
import pandas as pd
import re
from ioos_qc.config import QcConfig
from ioos_qc.qartod import qartod_compare, QartodFlags
from hakai_qc import hakai_tests, get, utils
import json


def tests_on_profiles(df,
                      hakai_stations,
                      qc_config,
                      timeseries_id='station',
                      profile_id='hakai_id',
                      direction_flag='direction_flag',
                      tinp='measurement_dt',
                      zinp='depth',
                      lon='longitude',
                      lat='latitude'
                      ):
    # This is just the indent to use when printing the executed tests.
    string_indent = 2 * ' '

    # Regroup profiles by profile_id and direction and sort them along zinp
    df = df.sort_values(by=[profile_id, direction_flag, zinp])

    # Loop through each  variables and profiles and apply QARTOD tests
    maximum_suspect_depth_ratio = qc_config['depth']['qartod']['gross_range_test']['maximum_suspect_depth_ratio']
    maximum_fail_depth_ratio = qc_config['depth']['qartod']['gross_range_test']['maximum_fail_depth_ratio']

    # Retrieve hakai tests parameters if provided in the config file
    if 'hakai_tests' in qc_config:
        hakai_tests_config = qc_config.pop('hakai_tests')
        hakai_tests_config = hakai_tests_config.pop('hakai')
    else:
        hakai_tests_config = {}

    # Find Flag values present in the data, attach a FAIL QARTOD Flag to them and replace them by NaN.
    #  Hakai database ingested some seabird flags -9.99E-29 which need to be recognized and removed.
    if 'bad_value_test' in hakai_tests_config:
        if hakai_tests_config['bad_value_test']['variable'] == 'all':
            columns_to_review = set(qc_config.keys()) - {'position'}
        else:
            columns_to_review = hakai_tests_config['bad_value_test']['columns_to_review']
        df = hakai_tests.bad_value_test(df, columns_to_review,
                                        flag_list=hakai_tests_config['bad_value_test']['flag_list'])

    # Run the tests for one station at the time and ignore rows that have pressure/depth flagged
    for station_name, station_df in df.dropna(axis=0, subset=['depth', 'pressure']).groupby(by=timeseries_id):
        print('QAQC ' + str(station_name))
        site_qc_config = qc_config

        # Review Site Lat/long and depth range if station is in Hakai List
        # Set Target Location for Range from Station test
        if station_name in hakai_stations.index:
            # Retrieve station info
            station_info = hakai_stations.loc[station_name]

            # Set latitude longitude acceptable range for the station
            # Use get_bbox_from_target_range utils tool
            # if station_info['Lat_DD'] and station_info['Long_DD']:
            #     site_qc_config['position']['qartod']['location_test']['bbox'] = \
            #         utils.get_bbox_from_target_range(
            #             station_info, site_qc_config['position']['qartod']['location_test']['target_range'])

            if station_info['Lat_DD'] and station_info['Long_DD'] and \
                    "target_range" in site_qc_config['position']['qartod']['location_test'].keys():
                site_qc_config['position']['qartod']['location_test']['target_lat'] = [station_info['Lat_DD']]
                site_qc_config['position']['qartod']['location_test']['target_lon'] = [station_info['Long_DD']]

            # Set Maximum Acceptable Depth and Pressure Based on Site Name
            if station_info['Bot_depth'] or station_info['Bot_depth_GIS']:
                max_depth = max([station_info['Bot_depth'], station_info['Bot_depth_GIS']])
                max_pressure = gsw.p_from_z(-max_depth, station_info['Lat_DD'])

                # Update Depth Config
                site_qc_config['depth']['qartod']['gross_range_test']['suspect_span'] = \
                    [0, max_depth * maximum_suspect_depth_ratio]
                site_qc_config['depth']['qartod']['gross_range_test']['fail_span'] = \
                    [0, max_depth * maximum_fail_depth_ratio]

                # Update Pressure Config
                site_qc_config['pressure']['qartod']['gross_range_test']['suspect_span'] = \
                    [0, max_pressure * maximum_suspect_depth_ratio]
                site_qc_config['pressure']['qartod']['gross_range_test']['fail_span'] = \
                    [0, max_pressure * maximum_fail_depth_ratio]

        # Run the rest of the tests one profile at the time
        for key, config in qc_config.items():
            # Print to follow what's happening
            print(string_indent + key)
            for test_type in config.keys():
                print(2 * string_indent + test_type)
                for test in config[test_type].items():
                    print(3 * string_indent + str(test))

            for index, unique_cast_df in station_df.groupby(by=[profile_id, direction_flag]):
                unique_cast_df = unique_cast_df.sort_values(zinp)

                qc = QcConfig(config)
                if key == 'position':
                    qc_results = qc.run(
                        tinp=unique_cast_df[tinp],
                        zinp=unique_cast_df[zinp],
                        lon=unique_cast_df[lon],
                        lat=unique_cast_df[lat])
                else:
                    qc_results = qc.run(
                        inp=unique_cast_df[key],
                        tinp=unique_cast_df[tinp],
                        zinp=unique_cast_df[zinp],
                        lon=unique_cast_df[lon],
                        lat=unique_cast_df[lat])

                # Add flag results to Data Frame
                for module, tests in qc_results.items():
                    for test, flag in tests.items():
                        df.loc[unique_cast_df.index,
                               key + '_' + module + '_' + test] = flag

    # HAKAI SPECIFIC TESTS #
    # This section regroup different non QARTOD tests which are specific to Hakai profile dataset. Most of the them
    # uses the pandas dataframe to transform the data and apply divers tests.
    # DO CAP DETECTION
    if any(df['direction_flag'] == 'u') and ('do_cap_test' in hakai_tests_config):
        for key in hakai_tests_config['do_cap_test']['variable']:
            print('DO Cap Detection to ' + key + ' variable')
            hakai_tests.do_cap_test(df, key,
                                    profile_id=profile_id,
                                    depth_var=zinp,
                                    direction_flag=direction_flag,
                                    bin_size=hakai_tests_config['do_cap_test']['bin_size'],
                                    suspect_threshold=hakai_tests_config['do_cap_test']['suspect_threshold'],
                                    fail_threshold=hakai_tests_config['do_cap_test']['fail_threshold'],
                                    ratio_above_threshold=hakai_tests_config['do_cap_test']['ratio_above_threshold'],
                                    minimum_bins_per_profile=hakai_tests_config['do_cap_test'][
                                        'minimum_bins_per_profile']
                                    )

    # Add a Missing Flag at Position when latitude/longitude are NaN. For some reasons, QARTOD is missing that.
    print('Flag Missing Position Records')
    df.loc[df['latitude'].isna(), 'position_unknown_location'] = QartodFlags.UNKNOWN
    df.loc[df['longitude'].isna(), 'position_unknown_location'] = QartodFlags.UNKNOWN

    # BOTTOM HIT DETECTION
    #  Find Profiles that were flagged near the bottom and assume this is likely related to having it the bottom.
    if 'bottom_hit_detection' in hakai_tests_config:
        print('Flag Bottom Hit Data')
        df = hakai_tests.bottom_hit_detection(df,
                                              variable=hakai_tests_config['bottom_hit_detection']['variable'],
                                              profile_id=profile_id,
                                              depth_variable=zinp,
                                              profile_direction_variable=direction_flag
                                              )

    # Detect PAR Shadow
    if 'par_shadow_test' in hakai_tests_config:
        print('Flag PAR Shadow Data')
        df = hakai_tests.par_shadow_test(df,
                                         variable=hakai_tests_config['par_shadow_test']['variable'],
                                         min_par_for_shadow_detection=hakai_tests_config['par_shadow_test'][
                                             'min_par_for_shadow_detection'],
                                         profile_id=profile_id,
                                         direction_flag=direction_flag,
                                         depth_var=zinp,
                                         )

    # APPLY QARTOD FLAGS FROM ONE CHANNEL TO OTHER AGGREGATED ONES
    # Generate Hakai Flags
    for var in qc_config.keys():
        print('Apply flag results to ' + var)
        extra_flags = ''

        # Extra flags that apply to all variables
        extra_flags = extra_flags + '|bottom_hit_test'
        extra_flags = extra_flags + '|position_qartod_location_test'
        extra_flags = extra_flags + '|pressure_qartod_gross_range_test|depth_qartod_gross_range_test'

        # Add Density Inversion to selected variables
        if var in ['temperature', 'salinity', 'conductivity']:
            extra_flags = extra_flags + '|sigma0_qartod_density_inversion_test'

        # Add DO Cap Flag
        if var in ['dissolved_oxygen_ml_l', 'rinko_ml_l']:
            extra_flags = extra_flags + '|' + var + '_do_cap_test'

        # Create Hakai Flag Columns
        df = get_hakai_flag_columns(df, var, extra_flags)

    # Apply Hakai Grey List
    # Grey List should overwrite the QARTOD Flags
    print('Apply Hakai Grey List')
    df = hakai_tests.grey_list(df)

    return df


def update_hakai_ctd_profile_data(hakai_id=None,
                                  json_input=None,
                                  output='json',
                                  filter_variables=True,
                                  output_meta=False):
    # Define dataframe
    df = pd.DataFrame()

    if hakai_id is not None:
        print('Retrieve Hakai_ID: ' + str(hakai_id))
        # Retrieve data through the API
        # Get Hakai CTD Data Download through the API
        if type(hakai_id) is list:
            hakai_id_str = ','.join(hakai_id)
        else:
            hakai_id_str = hakai_id

        # Let's just get the data from QU39
        filterUrl = 'hakai_id={' + hakai_id_str + '}&status!=MISCAST&limit=-1'

        # Filter variables
        if filter_variables:
            variable_lists = get.hakai_api_selected_variables()
            filterUrl = filterUrl + '&fields=' + ','.join(variable_lists)

        df, url, meta = get.hakai_ctd_data(filterUrl, get_columns_info=output_meta)

    elif json_input is not None:
        # Hakai API JSON string to a pandas dataframe
        df = pd.DataFrame(json_input)
    else:
        assert RuntimeError, 'update_hakai_ctd_profile_data is missing either a hakai_id or json string input.'

    if len(df) == 0:
        assert RuntimeError, 'No Data is available for this specific input'

    # Save the list of initial variables
    initial_variable_list = df.columns

    # Get Derived Variables
    df = utils.derived_ocean_variables(df)

    # Load default test parameters used right now!
    qc_config = get.json_config('hakai_ctd_profile.json')

    # Get Reference stations ( this should be changed to get it form the database or added to the data table
    hakai_stations = get.hakai_stations()

    # Run all of the tests on each available profile
    df = tests_on_profiles(df, hakai_stations, qc_config)

    # Isolate the Hakai Flags columns and output to a json string
    json_out = df[initial_variable_list].to_json(orient='records')
    # Pandas JSON also output empty values, to avoid those empty values, use the code below instead.
    # json_out = json.dump([row.dropna().to_dict() for index, row in df.drop('Unnamed: 0', axis=1).iterrows()],
    #                    f, indent=2)
    return json_out


def apply_qartod_flag(apply_to, reference, df_to_convert=None):
    """
    Apply QARTOD flags from a reference vector to another vector. The tool can handle multiple reference vectors and a
    dataframe input.
    """
    # Can handle a data frame input
    if type(apply_to) is list is list and type(reference) is list and df_to_convert is not None:
        for item in apply_to:
            qartod_vectors = [df_to_convert[qartod_vector].values for qartod_vector in reference]
            qartod_vectors.append(df_to_convert[item].values)

            df_to_convert[item] = qartod_compare(qartod_vectors)

        updated_flags = df_to_convert
    else:
        updated_flags = qartod_compare([apply_to, reference])

    return updated_flags


def get_hakai_flag_columns(df, var,
                           extra_flag_list='',
                           flag_values_to_consider=[2, 3, 4, 9],
                           level_1_flag_suffix='_qartod_flag',
                           level_2_flag_suffix='_flag_description'):
    # Retrieve each flags column associated to a variable
    var_flag_results = df.filter(regex=var + '_' + extra_flag_list)

    # Drop Hakai already existing flags, this will be dropped once we get the right flag columns
    #  available on the database side
    var_flag_results = var_flag_results.drop(var + '_flag', axis=1, errors='ignore')

    # Just consider flags associated with a flag value
    var_flag_results_reduced = var_flag_results[var_flag_results.isin(flag_values_to_consider)].dropna(how='all',
                                                                                                       axis=0)

    # Get Flag Description for failed flag
    df[var + level_2_flag_suffix] = pd.Series(var_flag_results_reduced.to_dict(orient='index')) \
        .astype('str').str.replace(r'\'[\w\_]+\': nan,*\s*|\.0', '')
    df[var + level_2_flag_suffix].replace(pd.NA, '', inplace=True)

    # Aggregate all flag columns together
    df[var + level_1_flag_suffix] = qartod_compare(var_flag_results.transpose().to_numpy())
    return df


def update_research_dataset(path_out=r'',
                            creator_name=None):
    ctd_qc_log_endpoint = 'eims/views/output/ctd_qc'
    df_qc, query_url = get.hakai_ctd_data('limit=-1', endpoint=ctd_qc_log_endpoint)

    # Filter QC log by keeping only the lines that have inputs
    df_qc = df_qc.loc[df_qc.filter(like='_flag').dropna(axis=0, how='all').index].set_index('hakai_id')

    # Generate NetCDFs
    for hakai_id, row in df_qc.iterrows():
        # Retrieve flag columns that starts with AV, drop trailing _flag
        var_to_save = row.filter(like='_flag').str.startswith('AV').dropna()
        var_to_save = var_to_save[var_to_save].rename(index=lambda x: re.sub('_flag$', '', x))

        if len(var_to_save.index) > 0:
            print('Save ' + hakai_id)
            # TODO add a step to identify reviewer and add reviewer review to history
            # TODO add an input to add a creator attribute.
            # TODO should we overwrite already existing files overwritten
            get.research_profile_netcdf(hakai_id, path_out,
                                        variable_list=var_to_save.index.tolist())
