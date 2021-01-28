import gsw
import pandas as pd
from ioos_qc.config import QcConfig
from ioos_qc.qartod import qartod_compare, QartodFlags
from hakai_qc import hakai_tests


def tests_on_profiles(df,
                      hakai_stations,
                      qc_config,
                      group_variables=['device_model', 'device_sn', 'ctd_file_pk', 'ctd_cast_pk', 'direction_flag'],
                      tinp='measurement_dt',
                      zinp='depth',
                      lon='longitude',
                      lat='latitude'
                      ):
    # This is just the indent to use when printing the executed tests.
    string_indent = 2 * ' '

    # Loop through each  variables and profiles and apply QARTOD tests
    maximum_suspect_depth_ratio = qc_config['depth']['qartod']['gross_range_test']['maximum_suspect_depth_ratio']
    maximum_fail_depth_ratio = qc_config['depth']['qartod']['gross_range_test']['maximum_fail_depth_ratio']

    # Find Flag values present in the data, attach a FAIL QARTOD Flag to them and replace them by NaN.
    #  Hakai database ingested some seabird flags -9.99E-29 which need to be recognized and removed.
    df = hakai_tests.bad_value_test(df, set(qc_config.keys()) - {'position'})

    # Run the tests for one station at the time and ignore rows that have pressure/depth flagged
    for station_name, station_df in df.dropna(axis=0, subset=['depth', 'pressure']).groupby(by='station'):
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

            for index, unique_cast_df in station_df.groupby(by=group_variables):
                unique_cast_df = unique_cast_df.sort_values('pressure')

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
    if any(df['direction_flag'] == 'u'):
        for key in ['dissolved_oxygen_ml_l', 'rinko_do_ml_l']:
            print('DO Cap Detection to ' + key + ' variable')
            hakai_tests.do_cap_test(df, key)

    # Add a Missing Flag at Position when latitude/longitude are NaN. For some reasons, QARTOD is missing that.
    print('Flag Missing Position Records')
    df.loc[df['latitude'].isna(), 'position_unknown_location'] = QartodFlags.UNKNOWN
    df.loc[df['longitude'].isna(), 'position_unknown_location'] = QartodFlags.UNKNOWN

    # BOTTOM HIT DETECTION
    #  Find Profiles that were flagged near the bottom and assume this is likely related to having it the bottom.
    print('Flag Bottom Hit Data')
    df = hakai_tests.bottom_hit_detection(df,
                                          flag_channel='sigma0_qartod_density_inversion_test',
                                          profile_group_variable='hakai_id',
                                          vertical_variable='depth',
                                          profile_direction_variable='direction_flag')

    # Detect PAR Shadow
    print('Flag PAR Shadow Data')
    df = hakai_tests.par_shadow_test(df)

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
    return df


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
