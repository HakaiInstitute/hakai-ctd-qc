import hakai_qc
from datetime import datetime as dt
import os

hakai_id = '080217_2017-01-08T18:03:05.167Z'
path_out = r'C:\Users\jessy\test'
creator_name = 'Jessy Barrette'
creator_url = 'hakai.org'
creator_email = 'jessy.barrette@hakai.org'

global_attributes = {'institution': 'Hakai Institute',
                     'project': 'Hakai Oceanography',
                     'title': 'Hakai Research CTD Profile: ' + hakai_id,
                     'summary': 'text describing that specific data',
                     'comment': '',
                     'infoUrl': 'hakai.org',
                     'keywords': 'conductivity,temperature,salinity,depth,pressure,dissolved oxygen',
                     'acknowledgment': 'Hakai Field Techniciens, research and IT groups',
                     'id': hakai_id,
                     'naming_authority': 'Hakai Instititute',
                     'created_date': str(dt.utcnow()),
                     'creator_name': creator_name,
                     'creator_url': creator_url,
                     'creator_email': creator_email,
                     'standard_name_vocabulary': 'CF 1.3',
                     'license': 'unknown',
                     'geospatial_lat_units': 'degrees_north',
                     'geospatial_lon_units': 'degrees_east'}

file_path = hakai_qc.get.research_profile_netcdf(hakai_id, path_out)