import pandas as pd
from loguru import logger
from sentry_sdk import set_context, set_tag

tags = ["work_area", "station", "device_sn", "hakai_id"]


def run_sentry_warnings(casts_data, casts, minimum_date=None):
    """Review qc result and return to sentry particular results that needs a special attention."""

    if minimum_date:
        # Filter out times prior to minimum date
        casts_data["start_dt"] = pd.to_datetime(casts_data["start_dt"])
        casts_data = casts_data.query("start_dt>@minimum_date")
        if casts_data.empty:
            return

    casts = casts.set_index("hakai_id")

    def _generate_sentry_warning(query, message):
        if query:
            drops = casts_data.query(query)[tags].drop_duplicates()
        else:
            drops = casts_data[tags].drop_duplicates()

        for _, row in drops.iterrows():
            for tag in tags:
                set_tag(tag, row[tag])
            set_context("character", casts.loc[row["hakai_id"]])
            logger.warning(message)

    logger.info("Run Sentry Warnings")
    # Bottom Hit
    _generate_sentry_warning(
        "bottom_hit_test==4 and direction_flag=='d'", "Instrument likely hit bottom"
    )

    # distance from station or maximum depth
    _generate_sentry_warning("location_flag_level_1==4", "Drop is far from station")
    _generate_sentry_warning(
        "depth_in_station_range_test==4", "Drop is too deep for that station"
    )

    # Significant density inversion not related to bottom hit
    _generate_sentry_warning(
        "sigma0_qartod_density_inversion_test==4 and bottom_hit_test!=4 and direction_flag=='d'",
        "A significant density inversion is present in the profile",
    )

    # DO cap detected
    _generate_sentry_warning(
        "rinko_do_ml_l_do_cap_test==4",
        "Secondary oxygen instrument Rinko seems to have been deployed with the cap on the unit.",
    )
    _generate_sentry_warning(
        "dissolved_oxygen_ml_l_do_cap_test==4",
        "Oxygen instrument seems to have been deployed with the cap on the unit.",
    )

    # Out of range data
    _generate_sentry_warning(
        "salinity_qartod_gross_range_test==4", "Salinity out of range"
    )
    _generate_sentry_warning(
        "temperature_qartod_gross_range_test==4", "Temperature out of range"
    )
    _generate_sentry_warning(
        "dissolved_oxygen_ml_l_qartod_gross_range_test==4",
        "Dissolved Oxygen out of range",
    )
    _generate_sentry_warning(
        "rinko_do_ml_l_qartod_gross_range_test==4", "Secondary Oxygen out of range"
    )
    # _generate_sentry_warning("par_qartod_gross_range_test==4","PAR out of range")
