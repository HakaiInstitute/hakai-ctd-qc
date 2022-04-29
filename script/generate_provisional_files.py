from hakai_api import Client
import hakai_profile_qc.review

import numpy as np
import pandas as pd
import argparse
import json
from tqdm import tqdm
import re
import os

def rename_columns(col):
    if col in ['direction_flag']:
        return col
    col = re.sub('_flag$','_flag_description',col)
    col = re.sub('_flag_level_1$','_qartod_flag',col)
    return col

CHUNK_SIZE=300
parser = argparse.ArgumentParser(description="QC Hakai CTD Profiles")
parser.add_argument(
    "--output_path",
    type=str,
    help="Which database is run the script",
    default="./output",
)
args = parser.parse_args()
output_path = args.output_path

# Connect to API
client = Client()

ctd_cast_endpoint = "/ctd/views/file/cast"
ctd_cast_data_endpoint = "/ctd/views/file/cast/data"

# Get Cast Data
processed_cast_filter = "processing_stage={8_rbr_processed,8_binAvg,9_qc_auto,10_qc_pi}&status==null&station_latitude!=null&station_longitude!=null&work_area={CALVERT,QUADRA,JOHNSTONE STRAIT}&limit=-1&fields=work_area,station,hakai_id"
url = f"{client.api_root}{ctd_cast_endpoint}?{processed_cast_filter}"
response = client.get(url)
df_stations = pd.DataFrame(response.json())
chunks = round(len(df_stations)/CHUNK_SIZE)
for chunk in np.array_split(df_stations, chunks):
    # Run QC tests
    df = hakai_profile_qc.review.run_tests(hakai_id=chunk['hakai_id'].to_list())
    
    # Replace flag values from seabird
    df = df.replace({-9.99E-29: np.nan})

    # Consider only downcast data
    df['direction_flag'] = df['direction_flag'].fillna('d')
    df = df.query("direction_flag=='d'") # Ignore up cast and static data in provisional dataset

    # Rename Columns 
    df.columns = [rename_columns(col) for col in df.columns]
    df = df.drop(columns=['weather','bin_stats'])

    # Save data grouped xarray datasets just to serve temporarily ERDDAP
    for (work_area,station,hakai_id),df_profile in tqdm(df.groupby(['work_area','station','hakai_id']),desc='Save each individual files:',unit='file'):
        dir_path = os.path.join(output_path,work_area,station)
        if not os.path.isdir(dir_path):
            os.makedirs(dir_path)
        file_output = os.path.join(dir_path, f"{hakai_id}.nc")
        df_profile.to_xarray().to_netcdf(file_output)