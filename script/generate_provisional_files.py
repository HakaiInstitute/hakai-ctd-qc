from hakai_api import Client
import hakai_profile_qc.review

import numpy as np
import pandas as pd
import argparse
import json
from tqdm import tqdm
import re

def rename_columns(col):
    col = re.sub('_flag$','_flag_description',col)
    col = re.sub('_flag_level_1$','_UQL',col)
    return col

# Connect to API
client = Client()

ctd_cast_endpoint = "/ctd/views/file/cast"
ctd_cast_data_endpoint = "/ctd/views/file/cast/data"

# Get Cast Data
processed_cast_filter = "processing_stage={8_rbr_processed,8_binAvg}&station_latitude!=null&station_longitude!=null&limit=1000&fields=station&distinct"
url = f"{client.api_root}{ctd_cast_endpoint}?{processed_cast_filter}"
response = client.get(url)
stations = pd.DataFrame(response.json())['station'].tolist()

for station in stations:
    df = hakai_profile_qc.review.run_tests(station=[station])
    # rename columns 
    df.columns = [rename_columns(col) for col in df.columns]
    df = df.drop(columns=['weather','bin_stats'])
    df.to_xarray().to_netcdf(f"{station}.nc")


