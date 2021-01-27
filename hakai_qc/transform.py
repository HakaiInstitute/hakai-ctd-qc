import pandas as pd
import numpy as np


def dataframe_to_erddap_xarray(df,
                               time_var='measurement_dt',
                               depth_var='depth',
                               lat='latitude',
                               lon='longitude',
                               timeseries_id=None,
                               profile_id=None,
                               trajectory_id=None,
                               global_attributes={},
                               variable_attributes={}
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
    ds.attrs.update(global_attributes)

    # Add Variable Attributes
    for var in variable_attributes:
        ds[var].attrs.update(variable_attributes[var])

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
        ds.attrs['cdm_profile_variables'] = ','.join(profile_id)
        ds.attrs['cdm_trajectory_variables'] = ','.join(trajectory_id)
    elif timeseries_id and profile_id:
        ds.attrs['cdm_data_type'] = 'TimeSeriesProfile'
        ds.attrs['cdm_profile_variables'] = ','.join(profile_id)
        ds.attrs['cdm_timeseries_variables'] = ','.join(timeseries_id)
    elif profile_id:
        ds.attrs['cdm_data_type'] = 'Trajectory'
        ds.attrs['cdm_trajectory_variables'] = ','.join(trajectory_id)
    elif timeseries_id:
        ds.attrs['cdm_data_type'] = 'TimeSeries'
        ds.attrs['cdm_timeseries_variables'] = ','.join(timeseries_id)
    elif profile_id:
        ds.attrs['cdm_data_type'] = 'Profile'
        ds.attrs['cdm_profile_variables'] = ','.join(profile_id)
    else:
        ds.attrs['cdm_data_type'] = 'Point'

    # Define spatial and time coverage attributes
    coverage_attributes = get_spatial_converage_attributes(ds,
                                                            time=time_var,
                                                            lat=lat,
                                                            lon=lon,
                                                            depth=depth_var)
    ds.attrs.update(coverage_attributes)

    return ds


def get_spatial_converage_attributes(ds,
                                     time='time',
                                     lat='latitude',
                                     lon='longitude',
                                     depth='depth',
                                     ):
    time_spatial_converage = {}
    # time
    if time in ds:
        time_spatial_converage.update({
            'time_coverage_start': str(ds['measurement_dt'].min().values),
            'time_coverage_end': str(ds['measurement_dt'].max().values),
            'time_coverage_duration': str((ds['measurement_dt'].max() -ds['measurement_dt'].min())
                                          .values/np.timedelta64(1, 's'))+' seconds'
        })

    # lat/long
    if lat in ds and lon in ds:
        time_spatial_converage.update({
            'geospatial_lat_min': ds[lat].min().values,
            'geospatial_lat_max': ds[lat].max().values,
            'geospatial_lon_min': ds[lon].min().values,
            'geospatial_lon_max': ds[lon].max().values
        })

    # depth coverage
    if depth in ds:
        time_spatial_converage.update({
            'geospatial_vertical_min': ds[depth].min().values,
            'geospatial_vertical_max': ds[depth].max().values
        })

    return time_spatial_converage
