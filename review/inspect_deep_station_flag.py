import pandas as pd
from dotenv import dotenv_values
from sqlalchemy import create_engine, text

config = dotenv_values(".env")  # load shared development variables
engine = create_engine(
    url="postgresql+psycopg2://",
    username=config["POSTGRES_USERNAME"],
    password=config["POSTGRES_PASSWORD"],
    host=config["POSTGRES_HOST"],
    port=config["POSTGRES_PORT"],
    database=config["POSTGRES_DATABASE_NAME"],
    connect_args={"connect_timeout": 120},
)


def get_station_range_depth_test():
    with open("review/get_depth_in_station_range_test_results.sql") as query_file:
        query = query_file.read()

    with engine.connect() as con:
        df = pd.read_sql(text(query), con=con)

    station_info = pd.read_csv(
        "hakai_profile_qc/StationLocations.csv", delimiter=";"
    ).rename(columns={"Station": "station"})
    df = station_info[["station", "Bot_depth", "Bot_depth_GIS"]].merge(
        df, how="right", on="station"
    )

    return df


if __name__ == "__main__":
    df = get_station_range_depth_test()
    with open("review/get_depth_in_station_range_test.md", "w") as result_file:
        df.to_markdown(result_file, index=False)
