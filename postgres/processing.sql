/* A few notes on how the processing is organized:

I found that the processing is bizarrely, shockingly slow if I attempt
to do it all in one query, but much faster if I split the processing
into multiple queries. To keep that speed in a single query I used
common table expressions (CTEs), which serve as optimization fences so
that postgres optimizes each query section separately.

I also split the processing by data source so that a new data file
does not require reprocessing all data. Therefore there are separate
materialized views for each source. */

CREATE OR REPLACE FUNCTION process_measurements(measurement_type_ids int[])
  RETURNS TABLE (
    measurement_type_id int,
    measurement_time timestamp,
    value numeric,
    flagged boolean
  ) as $$
  with calibrated_measurements as (
    /* 1) Calibration.

       This statement applies calibration adjustments if needed. It
       relies on functions from calibration.sql. */
    select c.measurement_type_id,
	   instrument_time,
	   record,
	   value,
	   case when has_calibration then apply_calib(c.measurement_type_id, value, instrument_time)
	   else value end as calibrated_value,
	   flagged,
	   valid_range,
	   mdl,
	   remove_outliers
      from measurements c
	     left join measurement_types m
		 on c.measurement_type_id=m.id
     where m.id = any(measurement_type_ids)
  ), measurement_medians as (
    /* 2) Running medians.

       This statement calculates running medians and running Median
       Absolute Deviations (MAD). It relies on functions from
       filtering.sql. These numbers are used to check for outliers. */
    select *,
	   case when remove_outliers then runmed(calibrated_value) over w
	   else null end as running_median,
	   case when remove_outliers then runmad(calibrated_value) over w
	   else null end as running_mad
      from calibrated_measurements
	     WINDOW w AS (partition by measurement_type_id
			  ORDER BY instrument_time
			  rows between 120 preceding and 120 following)
  )
  /* 3) QC checks.

     This statement checks for flag conditions. It relies on functions
     from flags.sql. The end result is the fully processed data. */
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


/* 4) Hourly aggragates.

This view aggregates the processed data by hour. It relies on a
function from flags.sql. */
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

/* To update the processed data, simply need to refresh the relevant materialized views. For example, to update WFMS campbell results:
refresh materialized view calibration_values;
refresh materialized view processed_campbell_wfms;
refresh materialized view hourly_campbell_wfms; */
