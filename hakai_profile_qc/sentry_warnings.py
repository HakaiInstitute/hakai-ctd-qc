from sentry_sdk import set_tag, set_context
import logging
logger = logging.getLogger(__name__)
tags = ['work_area','station','device_sn','hakai_id']
context = ["cruise","vessel","operators","comments","cast_type","no_cast","bottle_drop","processing_software_version"]
def run_sentry_warnings(casts_data,casts):
    casts = casts.set_index('hakai_id')
    def _generate_sentry_warning(query,message):
        if query:
            drops = casts_data.query(query)[tags].drop_duplicates()
        else:
            drops = casts_data[tags].drop_duplicates()

        for _, row in drops.iterrows():
            for tag in tags:
                set_tag(tag,row[tag])
            set_context("character",casts.loc[row['hakai_id']][context].to_dict())
            logger.warning(message)
            
    logger.info("Run Sentry Warnings")
    # Bottom Hit
    _generate_sentry_warning("bottom_hit_test==4 and direction_flag=='d'",'Instrument likely hit bottom')

    # distance from station or maximum depth
    _generate_sentry_warning("location_flag_level_1==4","Drop is far from station")
    _generate_sentry_warning("depth_in_station_range_test==4","Drop is too deep for that station")

    # Significant density inversion not related to bottom hit
    _generate_sentry_warning("sigma0_qartod_density_inversion_test==4 and bottom_hit_test!=4 and direction_flag=='d'","A significant density inversion is present in the profile")

    # DO cap detected
    _generate_sentry_warning("rinko_do_ml_l_do_cap_test==4","Secondary oxygen instrument Rinko seems to have been deployed with the cap on the unit.")
    _generate_sentry_warning("dissolved_oxygen_ml_l_do_cap_test==4","Oxygen instrument seems to have been deployed with the cap on the unit.")

    # Out of range data
    _generate_sentry_warning("salinity_qartod_gross_range_test==4","Salinity out of range")
    _generate_sentry_warning("temperature_qartod_gross_range_test==4","Temperature out of range")
    _generate_sentry_warning("dissolved_oxygen_ml_l_qartod_gross_range_test==4","Dissolved Oxygen out of range")
    _generate_sentry_warning("rinko_do_ml_l_qartod_gross_range_test==4","Secondary Oxygen out of range")



