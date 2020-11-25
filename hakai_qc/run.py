import gsw
from ioos_qc.config import QcConfig
from ioos_qc.qartod import qartod_compare

from hakai_qc import utils


def tests_on_profiles(df,
                      hakai_stations,
                      qc_config,
                      group_variables=['device_model', 'device_sn', 'ctd_file_pk', 'ctd_cast_pk', 'direction_flag']
                      ):
    string_indent = 2*' '

    # Loop through each  variables and profiles and apply QARTOD tests
    maximum_suspect_depth_ratio = qc_config['depth']['qartod']['gross_range_test']['maximum_suspect_depth_ratio']
    maximum_fail_depth_ratio = qc_config['depth']['qartod']['gross_range_test']['maximum_fail_depth_ratio']

    # Run the tests for one station at the time
    for station_name, station_df in df.groupby(by='station'):
        print('QAQC ' + str(station_name))
        site_qc_config = qc_config

        # Review Site Lat/long and depth range if station is in Hakai List
        # Set Target Location for Range from Station test
        if station_name in hakai_stations.index:
            # Retrieve station info
            station_info = hakai_stations.loc[station_name]

            # Set latitude longitude acceptable range for the station
            if station_info['Lat_DD'] and station_info['Long_DD']:
                site_qc_config['position']['qartod']['location_test']['bbox'] = \
                    utils.get_bbox_from_target_range(station_info,
                                                     site_qc_config['position']['qartod']['location_test']['range'])

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
            print(string_indent+key)
            for test_type in config.keys():
                print(2 * string_indent + test_type)
                for test in config[test_type].items():
                    print(3 * string_indent + str(test))

            for index, unique_cast_df in station_df.groupby(by=group_variables):
                qc = QcConfig(config)
                if key == 'position':
                    qc_results = qc.run(
                        tinp=unique_cast_df['depth'],
                        zinp=unique_cast_df['depth'],
                        lon=unique_cast_df['longitude'],
                        lat=unique_cast_df['latitude'])
                else:
                    qc_results = qc.run(
                        inp=unique_cast_df[key],
                        tinp=unique_cast_df['pressure'],
                        zinp=unique_cast_df['depth'],
                        lon=unique_cast_df['longitude'],
                        lat=unique_cast_df['latitude'])

                # Add the aggregated test results to the data frame
                if 'qartod' in qc_results:
                    df.loc[unique_cast_df.index, key + '_qartod_aggregate'] = qc_results['qartod']['aggregate']

                elif 'qartod_profile' in qc_results:
                    df.loc[unique_cast_df.index, key + '_qartod_aggregate'] = qc_results['qartod_profile'][
                        'density_inversion_test']

                # DO Cap Test
                if key in ['dissolved_oxygen_ml_l', 'rinko_do_ml_l']:
                    # Compare up and downcast to see if there's a significant difference recorded for each depth.
                    #  When the cap is left on the Rinko Sensor it's pretty obvious by comparing the up and downcast.
                    is_pk = df['ctd_cast_pk'] == index[3]
                    if any(df[is_pk]['direction_flag'] == 'u'):
                        mean_do_diff = df[is_pk].groupby(by='pressure')['dissolved_oxygen_ml_l'].diff().mean()

                        if mean_do_diff > 0.5:
                            df.loc[unique_cast_df.index, key + '_do_cap_flag'] = 4
                        if mean_do_diff > 0.2:
                            df.loc[unique_cast_df.index, key + '_do_cap_flag'] = 3
                        else:
                            df.loc[unique_cast_df.index, key + '_do_cap_flag'] = 1
                    else:
                        df.loc[unique_cast_df.index, key + '_do_cap_flag'] = 9
                # TODO apply sigma0 flag to CTD data
                # TODO apply bad pressure/depth to other data
                # TODO add do_cap_flag to qartod flags
                # TODO add a text description of the tests results for each profiles which can populate the drop
                #  comment: how many flagged 3, 4 or 9
    # APPLY QARTOD FLAGS FROM ONE CHANNEL TO OTHER AGGREGATED ONES
    # Apply hakai_flag_value to all corresponding qartod_aggregate flag if available
    for flag_value_column in df.filter(like='_hakai_flag_value').columns.to_list():
        qartod_aggregate_column = 'depth_hakai_flag_value'.replace('_hakai_flag_value', '') + '_qartod_aggregate'
        if qartod_aggregate_column in df.columns:
            df = apply_qartod_flag([qartod_aggregate_column], [flag_value_column], df_to_convert=df)

    # Apply Density Flags to Salinity, Conductivity and Temperature data
    df = apply_qartod_flag(['salinity_qartod_aggregate', 'temperature_qartod_aggregate'], ['sigma0_qartod_aggregate'],
                           df_to_convert=df)
    # Apply bottom hit flag to all qartod_aggregate flags
    df = apply_qartod_flag(df.filter(like='qartod_aggregate').columns.to_list(), ['bottom_hit_flag'],
                           df_to_convert=df)

    # Apply Pressure and Depth flag to all qartod_aggregate flags
    df = apply_qartod_flag(df.filter(like='qartod_aggregate').columns.to_list(),
                           ['pressure_qartod_aggregate', 'depth_qartod_aggregate'],
                           df_to_convert=df)

    # Apply Position Flag at all qartod_aggregate flags
    df = apply_qartod_flag(df.filter(like='qartod_aggregate').columns.to_list(), ['position_qartod_aggregate'],
                           df_to_convert=df)

    # Apply DO Cap Flag to oxygen qartod_aggregate flags
    df = apply_qartod_flag(['dissolved_oxygen_ml_l_qartod_aggregate'], ['dissolved_oxygen_ml_l_do_cap_flag'],
                           df_to_convert=df)
    df = apply_qartod_flag(['rinko_do_ml_l_qartod_aggregate'], ['rinko_do_ml_l_do_cap_flag'], df_to_convert=df)
    return df


    return df

    """