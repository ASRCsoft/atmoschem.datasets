/* A few notes on how the processing is organized:

I found that the processing is bizarrely, shockingly slow if I attempt
to do it all in one query, but much faster if I split the processing
into multiple queries. To keep that speed in a single query I used
common table expressions (CTEs), which serve as optimization fences so
that postgres optimizes each query section separately.

I also split the processing by data source so that a new data file
does not require reprocessing all data. Therefore there are separate
materialized views for each source. */

/* Apply calibration adjustments if needed, using functions from
   calibration.sql. */
create or replace view calibrated_measurements as
  select c.measurement_type_id,
	 instrument_time,
	 record,
	 value,
	 case when apply_ce then apply_calib(c.measurement_type_id, value, instrument_time) /
		interpolate_ce(c.measurement_type_id, instrument_time)
	 when has_calibration then apply_calib(c.measurement_type_id, value, instrument_time)
	 else value end as calibrated_value,
	 flagged,
	 valid_range,
	 mdl,
	 remove_outliers
    from measurements c
	   left join measurement_types m
	       on c.measurement_type_id=m.id;

CREATE OR REPLACE FUNCTION process_measurements(measurement_type_ids int[])
  RETURNS TABLE (
    measurement_type_id int,
    measurement_time timestamp,
    value numeric,
    flagged boolean
  ) as $$
  with calibrated_measurements_subset as (
    select *
      from calibrated_measurements
     where measurement_type_id = any(measurement_type_ids)
  ), measurement_medians as (
    /* Calculate running medians and running Median Absolute
       Deviations (MAD) using functions from filtering.sql. These
       numbers are used to check for outliers. */
    select *,
	   case when remove_outliers then runmed(calibrated_value) over w
	   else null end as running_median,
	   case when remove_outliers then runmad(calibrated_value) over w
	   else null end as running_mad
      from calibrated_measurements_subset
	     WINDOW w AS (partition by measurement_type_id
			  ORDER BY instrument_time
			  rows between 120 preceding and 120 following)
  )
  /* Check for flag conditions using functions from flags.sql. The end
     result is the fully processed data. */
  select measurement_type_id,
	 instrument_time as time,
	 calibrated_value as value,
	 is_flagged(measurement_type_id, null, instrument_time,
		    calibrated_value, flagged, running_median,
		    running_mad) as flagged
    from measurement_medians;
$$ language sql; 
  
CREATE materialized VIEW processed_campbell_wfms as
  select *
    from process_measurements((select array_agg(id)
				 from measurement_types
				where site_id=1));
create index processed_campbell_wfms_idx on processed_campbell_wfms(measurement_type_id, measurement_time);

CREATE materialized VIEW processed_campbell_wfml as
  select *
    from process_measurements((select array_agg(id)
				 from measurement_types
				where site_id=2));
create index processed_campbell_wfml_idx on processed_campbell_wfml(measurement_type_id, measurement_time);

/* Add derived measurements to the processed measurements. */
CREATE OR REPLACE FUNCTION get_measurement_id(int, text)
  RETURNS int as $$
  select id
    from measurement_types
   where site_id=$1
     and measurement=$2;
$$ language sql STABLE PARALLEL SAFE;

create or replace view wfms_no2 as
  select get_measurement_id(1, 'NO2'),
	 wfms_no.measurement_time,
	 (wfms_nox.value - wfms_no.value) /
	   interpolate_ce(get_measurement_id(1, 'NOx'),
			  wfms_no.measurement_time) as value,
	 wfms_nox.flagged or wfms_no.flagged as flagged
    from (select *
	    from processed_campbell_wfms
	   where measurement_type_id=get_measurement_id(1, 'NO')) wfms_no
	   join (select *
		   from processed_campbell_wfms
		  where measurement_type_id=get_measurement_id(1, 'NOx')) wfms_nox
	       on wfms_no.measurement_time=wfms_nox.measurement_time;

/* Combine all processed data. */
CREATE materialized VIEW processed_measurements as
  select * from processed_campbell_wfms
   union
  select * from wfms_no2
   union
  select * from processed_campbell_wfml;
create index processed_measurements_idx on processed_measurements(measurement_type_id, measurement_time);


/* Aggregate the processed data by hour using a function from
   flags.sql. */
CREATE materialized VIEW hourly_campbell_wfms as
  select measurement_type_id,
	 measurement_time,
	 value,
	 get_hourly_flag(measurement_type_id, value, n_values::int) as flag
    from (select measurement_type_id,
		 time_bucket('1 hour', measurement_time) as measurement_time,
		 avg(value) FILTER (WHERE not flagged) as value,
		 count(value) FILTER (WHERE not flagged) as n_values
	    from processed_campbell_wfms
	   group by measurement_type_id, time_bucket('1 hour', measurement_time)) c1;
create index hourly_campbell_wfms_idx on hourly_campbell_wfms(measurement_type_id, measurement_time);

CREATE materialized VIEW hourly_campbell_wfml as
  select measurement_type_id,
	 measurement_time,
	 value,
	 get_hourly_flag(measurement_type_id, value, n_values::int) as flag
    from (select measurement_type_id,
		 time_bucket('1 hour', measurement_time) as measurement_time,
		 avg(value) FILTER (WHERE not flagged) as value,
		 count(value) FILTER (WHERE not flagged) as n_values
	    from processed_campbell_wfml
	   group by measurement_type_id, time_bucket('1 hour', measurement_time)) c1;
create index hourly_campbell_wfml_idx on hourly_campbell_wfml(measurement_type_id, measurement_time);

/* To update the processed data, simply need to refresh the relevant
materialized views. For example, to update WFMS campbell results:

refresh materialized view calibration_values;
refresh materialized view processed_campbell_wfms;
refresh materialized view hourly_campbell_wfms; */
