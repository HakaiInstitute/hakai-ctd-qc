import pandas as pd
import numpy as np
from ioos_qc.qartod import QartodFlags


def do_cap_test(df,
                var,
                profile_id='hakai_id',
                depth_var='pressure',
                suspect_threshold=.2,
                fail_threshold=0.5,
                ratio_above_threshold=0.5,
                mininum_bins_per_profile=10,
                flag_name='_do_cap_test'
                ):
    """
    Hakai do_cap_test compare down and up cast values measured by an instrument at the same depth. The test compare
    the number of records that has a different value above the suspect or fail threshold versus the total number of
    similar value available for each cast. If n_suspect/n_values is above suspect/fail threshold, the whole profile
    is flagged accordingly.

    INPUTS:
    df: dataframe
    var: variable to review up/down cast values
    depth_var: variable describing the vertical coordinate
    suspect_threshold: suspect threshold value for detection |X_nu - X_nd|
    fail_threshold: suspect threshold value for detection |X_nu - X_nd|
    ratio_above_threshold: minimum threshold of fraction of suspect/fail binned value to consider to flag profile
    minimum_bins_per_profile: minimum amount of bins necessary to make the test usable.

    ASSUMPTIONS:
    As of now, the test assume that the input data is already bin averaged for either up or downcast.

    OUTPUT:
    The test will generate an extra column [var]_do_cap_test with QARTOD flag.
    """

    print('DO Cap Detection to ' + var + ' variable')
    # Count how many values are available for each profile and pressure bin and get their range max-min (ptp)
    profile_bin_stats = df.groupby(by=[profile_id, depth_var])[var].agg([np.ptp, 'count'])

    profile_bin_stats['is_missing'] = profile_bin_stats['ptp'].isnull() & \
                                      (profile_bin_stats['count'] == 0)  # no value available

    # Difference between values higher than thresholds
    profile_bin_stats['is_suspect'] = (profile_bin_stats['ptp'] > suspect_threshold) & \
                                      (profile_bin_stats['count'] > 1)
    profile_bin_stats['is_fail'] = (profile_bin_stats['ptp'] > fail_threshold) & \
                                   (profile_bin_stats['count'] > 1)
    profile_bin_stats['is_unknown'] = profile_bin_stats['count'] == 1  # Only downcast or upcast available

    # Sum each flag per depth bin for each profiles per profile
    profile_stats = profile_bin_stats.groupby(by=[profile_id]).sum()

    # Get the amount of the vertical bin available total and the amount with value in up and downcast
    profile_stats['nBinsPerProfile'] = profile_bin_stats['ptp'].replace({pd.NA: -1}).groupby(by=[profile_id]).count()
    profile_stats['nGoodBinsPerProfile'] = profile_bin_stats[profile_bin_stats['ptp'] > 0]['ptp'] \
        .groupby(by=[profile_id]).count()

    # Get Ratio of each flag generated vs the amount of bins available
    for flag_test in ['is_unknown', 'is_suspect', 'is_fail', 'is_missing']:
        profile_stats[flag_test + '_ratio'] = profile_stats[flag_test] / profile_stats['nGoodBinsPerProfile']

    # Detect profiles for which test can be applied (missing up or downcast or not enough vertical bins)
    unknown_profile_id = profile_stats.index[
        (profile_stats['nGoodBinsPerProfile'] < mininum_bins_per_profile) |
        (profile_stats['nGoodBinsPerProfile'].isnull())]

    # Get the list of index for each flag type
    suspect_profile_id = profile_stats[(profile_stats['is_suspect_ratio'] > ratio_above_threshold)].index
    fail_profile_id = profile_stats[(profile_stats['is_fail_ratio'] > ratio_above_threshold)].index
    missing_profile_id = profile_stats[(profile_stats['nGoodBinsPerProfile'].isnull()) &
                                       (profile_stats['is_missing'] == profile_stats['nBinsPerProfile'])].index

    # Start with everything passing
    df[var + flag_name] = QartodFlags.GOOD
    if any(suspect_profile_id):
        df.loc[df[profile_id].isin(suspect_profile_id), var + flag_name] = QartodFlags.SUSPECT
    if any(fail_profile_id):
        df.loc[df[profile_id].isin(fail_profile_id), var + flag_name] = QartodFlags.FAIL
    if any(unknown_profile_id):
        df.loc[df[profile_id].isin(unknown_profile_id), var + flag_name] = QartodFlags.UNKNOWN
    if any(missing_profile_id):
        df.loc[df[profile_id].isin(missing_profile_id), var + flag_name] = QartodFlags.MISSING
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
    df['bottom_hit_flag'] = QartodFlags.GOOD

    bottom_hit_id = df.sort_values(by=[profile_group_variable, profile_direction_variable, vertical_variable]) \
        .groupby(by=[profile_group_variable, profile_direction_variable]) \
        .last()[flag_channel].isin([QartodFlags.SUSPECT, QartodFlags.FAIL])

    # Now let's flag the consecutive data that are flagged in sigma0 near the bottom as bottom hit
    for hakai_id in bottom_hit_id[bottom_hit_id].reset_index()[profile_group_variable]:
        for index, df_bottom_hit in df[df[profile_group_variable] == hakai_id].groupby(by=[profile_group_variable,
                                                                                           profile_direction_variable]):
            # For each bottom hit find the deepest good record in density and flag everything else below as FAIL
            df.loc[df_bottom_hit[df_bottom_hit[vertical_variable] >
                                 df_bottom_hit[df_bottom_hit[flag_channel] == 1][vertical_variable].max()].index,
                   'bottom_hit_flag'] = QartodFlags.FAIL
    return df


def par_shadow_test(df,
                    min_par_for_shadow_detection=5,
                    par_variable='par',
                    flag_name='par_shadow_flag',
                    profile_id='hakai_id',
                    direction_flag='direction_flag',
                    depth_var='depth'
                    ):
    """
    The PAR shadow test assume that PAR values should always be increasing with shallower depths. The tool first
    sort the data along the pressure for each individual profiles and compute the cumulative maximum value recorded
    from the bottom to the surface. A PAR value is flagged as SUSPECT if the value is bigger than the
    min_par_for_shadow_detection provided value and lower than the cumulative maximum value.
    """
    # Detect PAR Shadow
    print('Flag PAR Shadow Data')
    df['par_cummax'] = df.sort_values(by=[profile_id, direction_flag, depth_var], ascending=False).groupby(
        by=[profile_id, direction_flag])[par_variable].cummax()

    df[flag_name] = QartodFlags.GOOD
    df.loc[(df[par_variable] < df['par_cummax']) & (
            df['par_cummax'] > min_par_for_shadow_detection), 'par_shadow_flag'] = QartodFlags.SUSPECT
    df.drop('par_cummax', axis=1, inplace=True)
    return df
