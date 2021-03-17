import hakai_qc
import re
from datetime import datetime as dt
import os

hakai_qc.get.research_profile_netcdf('018066_2017-06-13T22:41:14.167Z', r'D:\temp',
                                     overwrite=True,
                                     remove_empty_variables=False)
hakai_qc.run.update_research_dataset(
    r'D:\temp', overwrite=True)
# hakai_id = '080217_2017-01-08T18:03:05.167Z'
# path_out = r'C:\Users\jessy\test'
# creator_name = 'Jessy Barrette'
# creator_url = 'hakai.org'
# creator_email = 'jessy.barrette@hakai.org'
#
# global_attributes = {'institution': 'Hakai Institute',
#                      'project': 'Hakai Oceanography',
#                      'title': 'Hakai Research CTD Profile: ' + hakai_id,
#                      'summary': 'text describing that specific data',
#                      'comment': '',
#                      'infoUrl': 'hakai.org',
#                      'keywords': 'conductivity,temperature,salinity,depth,pressure,dissolved oxygen',
#                      'acknowledgment': 'Hakai Field Techniciens, research and IT groups',
#                      'id': hakai_id,
#                      'naming_authority': 'Hakai Instititute',
#                      'created_date': str(dt.utcnow()),
#                      'creator_name': creator_name,
#                      'creator_url': creator_url,
#                      'creator_email': creator_email,
#                      'standard_name_vocabulary': 'CF 1.3',
#                      'license': 'unknown',
#                      'geospatial_lat_units': 'degrees_north',
#                      'geospatial_lon_units': 'degrees_east'}
#
# ctd_qc_log_endpoint = 'eims/views/output/ctd_qc'
# df_qc, query_url = hakai_qc.get.hakai_ctd_data('limit=-1', endpoint=ctd_qc_log_endpoint)
#
# # Filter QC log by keeping only the lines that have inputs
# df_qc = df_qc.loc[df_qc.filter(like='_flag').dropna(axis=0, how='all').index].set_index('hakai_id')
#
# # Generate NetCDFs
# for hakai_id, row in df_qc.iterrows():
#     # Retrieve flag columns that starts with AV, drop trailing _flag
#     var_to_save = row.filter(like='_flag').str.startswith('AV').dropna()
#     var_to_save = var_to_save[var_to_save].rename(index=lambda x: re.sub('_flag$', '', x))
#     if len(var_to_save.index) > 0:
#         print('Save '+hakai_id)
#         file_path = hakai_qc.get.research_profile_netcdf(hakai_id, path_out, variable_list=var_to_save.index.tolist())