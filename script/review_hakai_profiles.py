from hakai_api import Client
import hakai_profile_qc.review

import pandas as pd


# Connect to API
client = Client()

# Run by default in development
api_root = "https://goose.hakai.org/api"

ctd_cast_endpoint = "/ctd/views/file/cast"
ctd_cast_data_endpoint = "/ctd/views/file/cast/data"

# Get Cast Data
processed_cast_filter = "processing_stage={8_rbr_processed,8_binAvg}&limit=1000"
url = f"{api_root}{ctd_cast_endpoint}?{processed_cast_filter}"
response = client.get(url)

df_casts = pd.DataFrame(response.json())

# If no data needs to be qaqc
if df_casts.empty:
    print("No Drop needs to be QC")
    exit()

# How many drops needs to be qaqc
print(f"{len(df_casts)} profiles needs to be processed")
all_hakai_ids = df_casts["hakai_id"].to_list()
hakai_id_group_size = 40
hakai_id_groups = [
    all_hakai_ids[i : i + hakai_id_group_size]
    for i in range(0, len(all_hakai_ids), hakai_id_group_size)
]
for hakai_id_group in hakai_id_groups:
    df_qced = hakai_profile_qc.review.run_tests(hakai_id=hakai_id_group)

    # Update casts to qced
    df_casts.loc[
        df_casts["hakai_id"].isin(hakai_id_group), "processing_stage"
    ] = "9_autoQCed"
    df_casts_to_upload = df_casts.loc[df_casts["hakai_id"].isin(hakai_id_group)]

    # Upload qc data to db
    print("Upload to database ctd.ctd.cast")
    # client.put(put_casts_endpoint, df_casts_to_upload)

    print("Upload to database ctd.ctd.cast_data flag data")
    # client.put(put_ctd_cast_data, df_qced)
