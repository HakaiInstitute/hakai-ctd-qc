select
    x.station,
    count(x.hakai_id) as n_profiles,
    string_agg(x.hakai_id, ',') as hakai_id_list
from (
    select distinct
        station,
        hakai_id
    from ctd.ctd_file_cast_data
    where temperature_flag like '%depth_in_station_range_test%'
    group by station, hakai_id) as x
group by x.station
order by n_profiles desc, x.station asc;
