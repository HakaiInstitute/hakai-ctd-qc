select
	bad_casts.station as station,
	bad_casts.n_profiles as flagged_profiles_count,
	total_drops_count,
	bad_casts.avg_max_depth as flagged_avg_max_depth,
	total_avg_max_depth,
	total_median_max_depth,
	std_max_depth as flagged_std_max_depth,
	total_std_max_depth
from
	(
	select
		station,
		count(x.hakai_id) as n_profiles,
		string_agg(x.hakai_id, ',') as hakai_id_list,
		avg(max_depth) as avg_max_depth,
		stddev(max_depth) as std_max_depth
	from
		(
		select
			distinct station,
			hakai_id,
			max(depth) as max_depth
		from
			ctd.ctd_file_cast_data
		where
			temperature_flag like '%depth_in_station_range_test%'
		group by
			station,
			hakai_id ) as x
	group by
		station) as bad_casts
left join (
	select
		station,
		count(hakai_id) as total_drops_count,
		avg(max_depth) as total_avg_max_depth,
		stddev(max_depth) as total_std_max_depth,
		PERCENTILE_DISC(0.5) within group(
		order by max_depth) as total_median_max_depth
	from
		(
		select
			distinct station,
			hakai_id,
			max(depth) as max_depth
		from
			ctd.ctd_file_cast_data
		group by
			station,
			hakai_id ) as y
	group by
		station) as total_casts
	on
	bad_casts.station = total_casts.station
order by
		bad_casts.n_profiles desc,
		bad_casts.station asc;