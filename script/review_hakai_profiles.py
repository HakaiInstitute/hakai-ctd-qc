from hakai_api import Client
import hakai_profile_qc.review

import numpy as np
import pandas as pd
import argparse
import json
from tqdm import tqdm

CHUNK_SIZE=100

def generate_process_flags_json(cast, data):
    return json.dumps(
        {
            "cast": json.loads(
                cast[
                    ["ctd_cast_pk", "hakai_id", "processing_stage", "process_error"]
                ].to_json()
            ),
            "ctd_data": json.loads(
                data.query(f"hakai_id=='{cast['hakai_id']}'")
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
                .to_json(orient="records")
            ),
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
chunks = round(len(df_casts)/CHUNK_SIZE)
# If no data needs to be qaqc
if df_casts.empty:
    print("No Drops needs to be QC")
    exit()
print(f"{len(df_casts)} needs to be qc!")

for chunk in np.array_split(df_casts, chunks):
    df_qced = hakai_profile_qc.review.run_tests(
        hakai_id=chunk["hakai_id"].to_list(), api_root=api_root
    )

    # Convert QARTOD to string temporarily
    qartod_columns = df_qced.filter(regex="_flag_level_1").columns
    df_qced[qartod_columns] = df_qced[qartod_columns].astype(str)
    df_qced = df_qced.replace({"": None})

    # Update casts to qced
    chunk["processing_stage"] = "9_qc_auto"
    chunk["process_error"] = chunk["process_error"].fillna("")
    for id, row in tqdm(
        chunk.iterrows(), desc="Upload flags", unit="profil", total=len(chunk)
    ):
        json_string = generate_process_flags_json(row, df_qced)
        response = client.post(
            f"{api_root}/ctd/process/flags/json/{row['ctd_cast_pk']}", json_string
        )
        if response.status_code != 200:
            print(f"Failed to update {row['hakai_id']}")
