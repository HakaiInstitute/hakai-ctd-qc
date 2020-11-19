import gsw
from ioos_qc.config import QcConfig

from hakai_qc import utils


def tests_on_profiles(df,
                      hakai_stations,
                      qc_config,
                      group_variables=['device_model', 'device_sn', 'ctd_file_pk', 'ctd_cast_pk', 'direction_flag']
                      ):

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
            print(key)
            print(config)
            for index, unique_cast_df in station_df.groupby(by=group_variables):
                qc = QcConfig(config)
                if key is 'position':
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

    return df
