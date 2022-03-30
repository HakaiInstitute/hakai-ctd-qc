from hakai_api import Client
import hakai_profile_qc.review

import numpy as np
import pandas as pd
import argparse
import json


def generate_process_flags_json(cast, data):
    return json.dumps(
        {
            "cast": cast[
                ["ctd_cast_pk", "hakai_id", "processing_stage", "process_error"]
            ].to_json(),
            "cast_data": data.query(f"hakai_id=='{cast['hakai_id']}'")
            .filter(regex="^ctd_data_pk$|_flag$|_flag_level_1$")
            .drop(
                columns=[
                    "direction_flag",
                    "process_flag",
                    "process_flag_level_1",
                    "location_flag",
                    "location_flag_level_1",
                ]
            )
            .to_json(orient="records"),
        }
    )


# Connect to API
client = Client()

# Parse input to script
parser = argparse.ArgumentParser(description="QC Hakai CTD Profiles")
parser.add_argument(
    "-server",
    type=str,
    help="Which database is run the script",
    default="goose",
)
args = parser.parse_args()
if "hecate" == args.server:
    api_root = client.api_root
elif "goose" == args.server:
    api_root = "https://goose.hakai.org/api"
else:
    raise RuntimeError("Unknown server!")

# Define endpoints
ctd_cast_endpoint = "/ctd/views/file/cast"
ctd_cast_data_endpoint = "/ctd/views/file/cast/data"

# Get Cast Data
processed_cast_filter = "processing_stage={8_rbr_processed,8_binAvg}&limit=1000"
url = f"{api_root}{ctd_cast_endpoint}?{processed_cast_filter}"
response = client.get(url)

df_casts = pd.DataFrame(response.json())

# If no data needs to be qaqc
if df_casts.empty:
    print("No Drops needs to be QC")
    exit()

for chunk in np.array_split(df_casts, 40):
    df_qced = hakai_profile_qc.review.run_tests(hakai_id=chunk["hakai_id"].to_list())

    # Update casts to qced
    chunk["processing_stage"] = "9_qc_auto"
    for id, row in chunk.iterrows():
        json_string = generate_process_flags_json(row, df_qced)
        client.put(f"{api_root}/process/flags/json/{row['cast_pk']}", json_string)
