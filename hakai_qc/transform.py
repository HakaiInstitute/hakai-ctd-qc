import pandas as pd


def dataframe_to_erddap_xarray(df,
                               time_var='measurement_dt',
                               depth_var='depth',
                               lat='latitude',
                               lon='longitude',
                               station_id=None,
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
    if station_id:
        ds[station_id].attrs['cf_role'] = 'station_id'
    if profile_id:
        ds[profile_id].attrs['cf_role'] = 'profile_id'
    if trajectory_id:
        ds[trajectory_id].attrs['cf_role'] = 'trajectory_id'

    # Define feature type
    if station_id and profile_id and trajectory_id:
        print('No geometry exist with: station_id, profile_id, and trajectory_id defined.')
    elif profile_id and trajectory_id:
        ds.attrs['featureType'] = 'trajectoryProfile'
    elif station_id and profile_id:
        ds.attrs['featureType'] = 'timeSeriesProfile'
    elif profile_id:
        ds.attrs['featureType'] = 'trajectory'
    elif station_id:
        ds.attrs['featureType'] = 'timeSeries'
    elif profile_id:
        ds.attrs['featureType'] = 'profile'
    else:
        ds.attrs['featureType'] = 'point'

    # Define spatial and time coverage attributes
    converage_attributes = get_spatial_converage_attributes(ds,
                                                            time=time_var,
                                                            lat=lat,
                                                            lon=lon,
                                                            depth=depth_var)
    ds.attrs.update(converage_attributes)

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
            'time_converage_start': ds['measurement_dt'].min(),
            'time_converage_end': ds['measurement_dt'].max(),
            'time_converage_duration': ds['measurement_dt'].max() - ds['measurement_dt'].min()
        })

    # lat/long
    if lat in ds and lon in ds:
        time_spatial_converage.update({
            'geospatial_lat_min': ds[lat].min(),
            'geospatial_lat_max': ds[lat].max(),
            'geospatial_lon_min': ds[lon].min(),
            'geospatial_lon_max': ds[lon].max()
        })
    # depth coverage
    if depth in ds:
        time_spatial_converage.update({
            'geospatial_vertical_min': ds[depth].min(),
            'geospatial_vertical_max': ds[depth].max()
        })

    return time_spatial_converage
