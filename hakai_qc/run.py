import gsw
import pandas as pd
import numpy as np
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

    # Find Flag values present in the data, attach a FAIL QARTOD Flag to them and replace them by NaN.
    #  Hakai database ingested some seabird flags -9.99E-29 which need to be recognized and removed.
    flag_list = [-9.99E-29]
    columns_to_flag = set(qc_config.keys())-set(['position'])
    for flag in flag_list:
        for column in columns_to_flag:
            if any(df[column] == flag):
                df[column+'_hakai_flag_value'] = 1
                df.loc[df[column] == flag, column + '_hakai_flag_value'] = 4
    df = df.replace(flag_list, pd.NA)

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
                    df.loc[unique_cast_df.index, key + '_qartod_aggregate'] = qc_results['qartod']['aggregate']\
                        .astype(int)
                if 'argo' in qc_results:
                    # Add every argo tests to the data frame
                    for test in qc_results['argo'].keys():
                        df.loc[unique_cast_df.index, key + '_argo_'+test] = qc_results['argo'][test] \
                            .astype(int)
                # TODO add a text description of the tests results for each profiles which can populate the drop
                #  comment: how many flagged 3, 4 or 9

            # DO CAP DETECTION
            do_cap_suspect_threshold = .2
            do_cap_fail_threshold = .5
            ratio_above_treshold = .5
            min_n_bins = 10

            if key in ['dissolved_oxygen_ml_l', 'rinko_do_ml_l']:
                df[key+'_do_cap_flag'] = 9
                profile_do_compare = df.groupby(['hakai_id', 'pressure'])['dissolved_oxygen_ml_l'].agg(
                    [np.ptp, 'count'])

                profile_do_compare['is_suspect'] = profile_do_compare['ptp'] > do_cap_suspect_threshold
                profile_do_compare['is_fail'] = profile_do_compare['ptp'] > do_cap_fail_threshold

                profile_compare_results = profile_do_compare.groupby(by=['hakai_id',
                                                                         'is_suspect',
                                                                         'is_fail'])['ptp']\
                    .agg(['median', 'count']).unstack(['is_suspect', 'is_fail'])
                n_bins_per_profile = profile_compare_results['count'].sum(axis=1)

                suspect_hakai_id = profile_compare_results[(profile_compare_results['count'][True]
                                                            .sum(axis=1)/n_bins_per_profile > ratio_above_treshold) &
                                                           (n_bins_per_profile > min_n_bins)].index
                fail_hakai_id = profile_compare_results[(profile_compare_results['count'][True][True]/n_bins_per_profile
                                                         > ratio_above_treshold) &
                                                        (n_bins_per_profile > min_n_bins)].index

                if any(suspect_hakai_id):
                    df.loc[df['hakai_id'].isin(suspect_hakai_id), key + '_do_cap_flag'] = 3
                if any(fail_hakai_id):
                    df.loc[df['hakai_id'].isin(fail_hakai_id), key + '_do_cap_flag'] = 4

    # Add a Missing Flag at Position when latitude/longitude are NaN. For some reasons, QARTOD is missing that.
    df.loc[df['latitude'].isna(), 'position_qartod_aggregate'] = 9
    df.loc[df['longitude'].isna(), 'position_qartod_aggregate'] = 9

    # BOTTOM HIT DETECTION
    #  Find Profiles that were flagged near the bottom and assume this is likely related to having it the bottom.
    df = bottom_hit_detection(df,
                              flag_channel='sigma0_argo_density_inversion_test',
                              profile_group_variable='hakai_id',
                              vertical_variable='depth',
                              profile_direction_variable='direction_flag')

    # Detect PAR Shadow
    min_par_for_shadow_detection = 5
    df['par_cummax'] = df.sort_values(by=['hakai_id', 'direction_flag', 'depth'], ascending=False).groupby(
        by=['hakai_id', 'direction_flag'])['par'].cummax()

    df['par_shadow_flag'] = 1
    df.loc[(df['par'] < df['par_cummax']) & (df['par_cummax'] > min_par_for_shadow_detection), 'par_shadow_flag'] = 3
    df = apply_qartod_flag(['par_qartod_aggregate'], ['par_shadow_flag'], df)

    # APPLY QARTOD FLAGS FROM ONE CHANNEL TO OTHER AGGREGATED ONES
    # Apply hakai_flag_value to all corresponding qartod_aggregate flag if available
    for flag_value_column in df.filter(like='_hakai_flag_value').columns.to_list():
        qartod_aggregate_column = 'depth_hakai_flag_value'.replace('_hakai_flag_value', '') + '_qartod_aggregate'
        if qartod_aggregate_column in df.columns:
            df = apply_qartod_flag([qartod_aggregate_column], [flag_value_column], df_to_convert=df)

    # Apply Density Flags to Salinity, Conductivity and Temperature data
    if 'sigma0_argo_density_inversion_test' in df.columns:
        df = apply_qartod_flag(['salinity_qartod_aggregate', 'temperature_qartod_aggregate'],
                               ['sigma0_argo_density_inversion_test'], df_to_convert=df)

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


def bottom_hit_detection(df,
                         flag_channel,
                         profile_group_variable='hakai_id',
                         vertical_variable='depth',
                         profile_direction_variable='direction_flag'):
    """
    Method that flag consecutive data near the bottom of a profile that was flagged SUSPECT=3 or FAIl=4. Output a
    'bottom_hit_flag' channel.
    """

    # For each profile (down and up cast), get the density flag value for the deepest record.
    #  If flagged [3,4], it has likely hit the bottom.
    df['bottom_hit_flag'] = 1

    bottom_hit_id = df.sort_values(by=[profile_group_variable, profile_direction_variable, vertical_variable]) \
        .groupby(by=[profile_group_variable, profile_direction_variable]) \
        .last()[flag_channel].isin([3, 4])

    # Now let's flag the consecutive data that are flagged in sigma0 near the bottom as bottom hit
    for hakai_id in bottom_hit_id[bottom_hit_id].reset_index()[profile_group_variable]:
        for index, df_bottom_hit in df[df[profile_group_variable] == hakai_id].groupby(by=[profile_group_variable,
                                                                                           profile_direction_variable]):
            # For each bottom hit find the deepest good record in density and flag everything else below as FAIL
            df.loc[df_bottom_hit[df_bottom_hit[vertical_variable] >
                                 df_bottom_hit[df_bottom_hit[flag_channel] == 1][vertical_variable].max()].index,
                   'bottom_hit_flag'] = 4
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

