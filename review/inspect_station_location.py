import argparse
import pandas as pd

consider_columns = {
    "Lat_DD",
    "Long_DD",
    "Depth",
}


def main(station_list1, station_list2):
    df1 = (
        pd.read_csv(station_list1, sep=";", index_col=["Station"])
        .sort_index()
        .rename(columns={"Bot_depth_GIS": "Depth", "depth_src": "Depth Source"})[
            consider_columns
        ]
    )
    df2 = pd.read_csv(station_list2, sep=";", index_col=["Station"]).sort_index()[
        consider_columns
    ]

    df_cmp = df1.compare(df2, result_names=("first", "second"))
    df_cmp["delta_depth"] = df_cmp[("Depth", "first")] - df_cmp[("Depth", "second")]
    print(f"columns: \n{df_cmp.columns}")
    print(f"Comparisson: \n{df_cmp}")
    df_cmp.loc[(df_cmp["delta_depth"] < -2) | (df_cmp["delta_depth"] > 2)].to_markdown(
        "station_compare.md"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser("Compare two stationLocation.csv")
    parser.add_argument("station_list1")
    parser.add_argument("station_list2")
    args = parser.parse_args()

    main(args.station_list1, args.station_list2)
