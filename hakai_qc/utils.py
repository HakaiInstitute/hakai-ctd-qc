import gsw
from pyproj import Geod


def derived_ocean_variables(df):
    # Compute Derived Variables with TEOS-10 equations
    df['absolute salinity'] = gsw.SA_from_SP(df['salinity'], df['pressure'], df['longitude'], df['latitude'])
    df['conservative temperature'] = gsw.CT_from_t(df['absolute salinity'], df['temperature'], df['pressure'])
    df['density'] = gsw.rho(df['absolute salinity'], df['conservative temperature'], df['pressure'])
    df['sigma0'] = gsw.sigma0(df['absolute salinity'], df['conservative temperature'])
    return df


def get_bbox_from_target_range(station_info,
                               distance=1000):
    """
    Little function that gives back a square region centred on a target region for
    which sides is a defined distance away from the middle.
    """
    g = Geod(ellps='WGS84')

    upper_limit = g.fwd(station_info['Long_DD'], station_info['Lat_DD'], 0, distance)
    lower_limit = g.fwd(station_info['Long_DD'], station_info['Lat_DD'], 180, distance)
    east_limit = g.fwd(station_info['Long_DD'], station_info['Lat_DD'], 90, distance)
    west_limit = g.fwd(station_info['Long_DD'], station_info['Lat_DD'], 270, distance)

    # Define the bounding box
    bbox = [west_limit[0], lower_limit[1], east_limit[0], upper_limit[1]]
    return bbox
