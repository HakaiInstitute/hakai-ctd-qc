import hakai_qc
import os
import pandas as pd
from datetime import date, timedelta

"""
This is a temporary method to create netcdf files to be used for the provisional dataset 
within Hakai ERDDAP until the actual data gets within the database.
"""

output_path = 'D:/Temp'
start_date = date(2012, 6, 1)
end_date = date(2022, 1, 1)
delta = timedelta(days=30)


# Get station loop through eachs ta
# Load Hakai Station List
hakai_stations_gis = hakai_qc.get.hakai_stations().reset_index()

# Get Station lists from the ctd data
station_list_ctd, url, metadata = hakai_qc.get.hakai_ctd_data('limit=-1&fields=station&distinct')
station_list_ctd = station_list_ctd.sort_values(by='station').dropna()

hakai_stations_gis = hakai_stations_gis.loc[hakai_stations_gis['name'].isin(station_list_ctd['station'].tolist())]\
    .dropna(subset=['latitude', 'longitude']).set_index('name').sort_index()

# Get Hakai CTD Data Download through the API
variable_lists = hakai_qc.get.hakai_api_selected_variables()

group_variables = ['device_model', 'device_sn', 'ctd_file_pk', 'ctd_cast_pk', 'direction_flag']

for station, row in hakai_stations_gis.iterrows():
    print(station)
    # Let's just get the data from QU39
    filterUrl = 'limit=-1' + \
                '&station=' + station + \
                '&status!=MISCAST' + \
                '&start_dt>=' + start_date.strftime("%Y-%m-%d") + \
                '&start_dt<' + end_date.strftime("%Y-%m-%d") + \
                '&fields=' + ','.join(variable_lists)
    df, url, metadata = hakai_qc.get.hakai_ctd_data(filterUrl)

    # Regroup profiles and sort them by pressure
    df = df.sort_values(by=group_variables + ['pressure'])

    # Get Derived Variables
    df = hakai_qc.utils.derived_ocean_variables(df)

    # Load default test parameters used right now!
    qc_config = hakai_qc.get.json_config('hakai_ctd_profile.json')

    # Run all of the tests on each available profile
    df = hakai_qc.run.tests_on_profiles(df, hakai_stations_gis, qc_config)

    # Drop empty columns
    df = df.dropna(axis='columns', how='all')

    # Replace time columns by datetime
    for time_var in df.filter(regex='_dt$').columns:
        df[time_var] = pd.to_datetime(df[time_var])
    # Detect start and end times
    first_record = df['measurement_dt'].min()
    last_record = df['measurement_dt'].max()

    # Add station location ( will be use as officiel lat/long)
    df['latitude_station'] = row['latitude']
    df['longitude_station'] = row['longitude']

    ds = hakai_qc.transform.dataframe_to_erddap_xarray(df,
                                                       flag_columns={'_qartod_flag$': ['QARTOD', 'aggregate_quality_flag']})
    ds.to_netcdf(os.path.join(output_path, 'Provisional', 'HakaiProfileWaterProperties_Provisional_' + station + '_' +
                              first_record.strftime("%Y-%m-%d") + '_to_' +
                              last_record.strftime("%Y-%m-%d")+'.nc'))
