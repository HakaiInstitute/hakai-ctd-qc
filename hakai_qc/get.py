import pandas as pd


def get_hakai_stations():
    # Load Hakai Station List
    hakai_stations = pd.read_csv(r'../HakaiStationLocations.csv', sep=';', index_col='Station')

    # Index per station name
    hakai_stations.index = hakai_stations.index.str.upper()  # Force Site Names to be uppercase

    # Convert Depth columns to float values
    hakai_stations['Bot_depth'] = pd.to_numeric(hakai_stations['Bot_depth'], errors='coerce').astype(float)
    hakai_stations['Bot_depth_GIS'] = pd.to_numeric(hakai_stations['Bot_depth_GIS'], errors='coerce').astype(float)

    return hakai_stations
