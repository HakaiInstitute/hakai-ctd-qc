import pandas as pd
import numpy as np
import re


def dataframe_to_erddap_xarray(df,
                               time_var='measurement_dt',
                               depth_var='depth',
                               lat='latitude',
                               lon='longitude',
                               timeseries_id=None,
                               profile_id=None,
                               trajectory_id=None,
                               global_attributes: dict = None,
                               variable_attributes: dict = None,
                               flag_columns: dict = None
                               ):
    # Retrieve datetime and datetimetz variables in the dataframe
    datetime_variables = df.select_dtypes('datetime').columns
    datetimetz_variables = df.select_dtypes('datetimetz').columns

    # Convert datetimetz to UTC and drop timezone
    for var in datetimetz_variables:
        df[var] = df[var].dt.tz_convert('UTC').dt.tz_localize(None)

    # Convert Object (we'll assume it's all Strings) to |S variables
    for var in df.select_dtypes(object).columns:
        df[var] = df[var].replace({pd.NA: ''}).astype(str).str.encode("ascii", errors='ignore').astype('|S')

    # Convert int64 to int
    for var in df.select_dtypes('int64').columns:
        df[var] = df[var].astype(int)

    # Drop rows with unknown index
    df = df.loc[df.index.dropna()]

    # With all the conversions completed convert the dataframe to an xarray
    ds = df.to_xarray()

    # Add time variables encoding and units
    for var in datetimetz_variables:
        if var in ds:
            ds[var].encoding['units'] = 'seconds since 1970-01-01T00:00:00Z'
            ds[var].attrs['timezone'] = 'UTC'

    # Not timezone aware variables
    for var in datetime_variables:
        if var in ds:
            ds[var].encoding['units'] = 'seconds since 1970-01-01T00:00:00'

    # Add Global Attributes
    if global_attributes is not None:
        ds.attrs.update(global_attributes)

    # Add Variable Attributes
    if variable_attributes is not None:
        for var in variable_attributes:
            if var in ds:
                ds[var].attrs.update(variable_attributes[var])

    # Add Flag related attributes
    if flag_columns:
        for flag_regex, flag_info in flag_columns.items():
            for var in ds.keys():
                if re.search(flag_regex, var):
                    associated_var = re.sub(flag_regex, '', var)
                    flag_dict = {'long_name': [associated_var[0].capitalize() +associated_var[1:] +
                                            ' Summary QC Flag']}
                    if len(flag_info) > 1:
                        flag_dict.update({'standard_name': flag_info[1]})

                    if flag_info[0] == 'QARTOD':
                        flag_dict.update({'missing_value': 2,
                                        'flag_meaning': "PASS NOT_EVALUATED SUSPECT FAIL MISSING",
                                        'flag_values': [1, 2, 3, 4, 9]})
                    ds[var].attrs.update(flag_dict)

                    if associated_var in ds:
                        if 'ancillary_variables' in ds[associated_var].attrs:
                            ds[associated_var].attrs['ancillary_variables'] = ds[associated_var].attrs[
                                                                                'ancillary_variables'] + ' ' + var
                        else:
                            ds[associated_var].attrs.update({'ancillary_variables': var})

    # Add CF Roles
    if timeseries_id:
        ds[timeseries_id].attrs['cf_role'] = 'timeseries_id'
    if profile_id:
        ds[profile_id].attrs['cf_role'] = 'profile_id'
    if trajectory_id:
        ds[trajectory_id].attrs['cf_role'] = 'trajectory_id'

    # Define feature type
    if timeseries_id and profile_id and trajectory_id:
        print('No geometry exist with: timeseries_id, profile_id, and trajectory_id defined.')
    elif profile_id and trajectory_id:
        ds.attrs['cdm_data_type'] = 'TrajectoryProfile'
        ds.attrs['cdm_profile_variables'] = profile_id
        ds.attrs['cdm_trajectory_variables'] = trajectory_id
    elif timeseries_id and profile_id:
        ds.attrs['cdm_data_type'] = 'TimeSeriesProfile'
        ds.attrs['cdm_profile_variables'] = profile_id
        ds.attrs['cdm_timeseries_variables'] = timeseries_id
    elif profile_id:
        ds.attrs['cdm_data_type'] = 'Trajectory'
        ds.attrs['cdm_trajectory_variables'] = trajectory_id
    elif timeseries_id:
        ds.attrs['cdm_data_type'] = 'TimeSeries'
        ds.attrs['cdm_timeseries_variables'] = timeseries_id
    elif profile_id:
        ds.attrs['cdm_data_type'] = 'Profile'
        ds.attrs['cdm_profile_variables'] = profile_id
    else:
        ds.attrs['cdm_data_type'] = 'Point'

    # Define spatial and time coverage attributes
    ds.attrs.update(get_spatial_coverage_attributes(ds, time=time_var, lat=lat, lon=lon, depth=depth_var))

    return ds


def get_spatial_coverage_attributes(ds,
                                    time='time',
                                    lat='latitude',
                                    lon='longitude',
                                    depth='depth',
                                    ):
    time_spatial_coverage = {}
    # time
    if time in ds:
        time_spatial_coverage.update({
            'time_coverage_start': str(ds['measurement_dt'].min().values),
            'time_coverage_end': str(ds['measurement_dt'].max().values),
            'time_coverage_duration': str((ds['measurement_dt'].max() - ds['measurement_dt'].min())
                                          .values / np.timedelta64(1, 's')) + ' seconds'
        })

    # lat/long
    if lat in ds and lon in ds:
        time_spatial_coverage.update({
            'geospatial_lat_min': ds[lat].min().values,
            'geospatial_lat_max': ds[lat].max().values,
            'geospatial_lon_min': ds[lon].min().values,
            'geospatial_lon_max': ds[lon].max().values
        })

    # depth coverage
    if depth in ds:
        time_spatial_coverage.update({
            'geospatial_vertical_min': ds[depth].min().values,
            'geospatial_vertical_max': ds[depth].max().values
        })

    return time_spatial_coverage
