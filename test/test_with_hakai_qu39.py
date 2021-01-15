import hakai_qc

# Load Hakai Station List
hakai_stations = hakai_qc.get.hakai_stations()

# Get Hakai CTD Data Download through the API
station = 'QU39'
variable_lists = hakai_qc.get.hakai_api_selected_variables()

# Let's just get the data from QU39
filterUrl = 'station='+station+'&status!=MISCAST&limit=-1'+'&fields='+','.join(variable_lists)
df, url = hakai_qc.get.hakai_ctd_data(filterUrl)
print(str(len(df))+' records found')

# Regroup profiles and sort them by pressure
group_variables = ['device_model', 'device_sn', 'ctd_file_pk', 'ctd_cast_pk', 'direction_flag']
df = df.sort_values(by=group_variables+['pressure'])

# Get Derived Variables
df = hakai_qc.utils.derived_ocean_variables(df)

# Load default test parameters used right now!
qc_config = hakai_qc.get.json_config('hakai_ctd_profile.json')

# Run all of the tests on each available profile
df = hakai_qc.run.tests_on_profiles(df, hakai_stations, qc_config)

# spike_test review hakai id
# 080217_2017-01-05T17:32:36.333Z