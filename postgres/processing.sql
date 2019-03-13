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
	       on c.measurement_type_id=m.id
   where apply_processing;

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

-- join two measurement types to make it easy to derive values from
-- multiple measurements
CREATE OR REPLACE FUNCTION combine_measures(measurement_type_id1 int, measurement_type_id2 int)
  RETURNS TABLE (
    measurement_time timestamp,
    value1 numeric,
    value2 numeric,
    flagged1 boolean,
    flagged2 boolean
  ) as $$
  declare
  q text := '
    select m1.measurement_time,
	   m1.value,
	   m2.value,
	   coalesce(m1.flagged, false),
	   coalesce(m2.flagged, false)
      from (select *
	      from %I
	     where measurement_type_id=$1) m1
	     join (select *
		     from %I
		    where measurement_type_id=$2) m2
		 on m1.measurement_time=m2.measurement_time';
  data_source text;
  begin
    -- get the processed data view name using the data source and site
    select 'processed_' ||
	     m1.data_source ||
	     '_' || lower(sites.short_name) into data_source
      from measurement_types m1
	     join sites
		 on m1.site_id=sites.id
     where m1.id=measurement_type_id1;
    RETURN QUERY EXECUTE format(q, data_source, data_source)
      USING measurement_type_id1, measurement_type_id2;
  end;
$$ language plpgsql;

create or replace view wfms_no2 as
  select measurement_type_id,
	 measurement_time,
	 value,
	 flagged or
	   is_outlier(value, runmed(value) over w,
		      runmad(value) over w) as flagged
    from (select get_measurement_id(1, 'NO2') as measurement_type_id,
		 measurement_time,
		 (value2 - value1) /
		   interpolate_ce(get_measurement_id(1, 'NOx'),
				  measurement_time) as value,
		 flagged1 or flagged2 as flagged
	    from combine_measures(get_measurement_id(1, 'NO'),
				  get_measurement_id(1, 'NOx'))) cm1
	   WINDOW w AS (partition by measurement_type_id
			ORDER BY measurement_time
			rows between 120 preceding and 120 following);

create or replace view wfms_slp as
  select get_measurement_id(1, 'SLP'),
	 measurement_time,
	 value1 *
	   (1 - .0065 * 1483.5 /
	   (value2 + .0065 * 1483.5 + 273.15))^(-5.257) as value,
	 flagged1 or flagged2 as flagged
    from combine_measures(get_measurement_id(1, 'BP'),
			  get_measurement_id(1, 'PTemp_C'));

create or replace view wfms_ws as
  select get_measurement_id(1, 'WS'),
	 measurement_time,
	 greatest(case when not flagged1 then value1
		  else null end,
		  case when not flagged2 then value2
		  else null end) as value,
	 flagged1 and flagged2 as flagged
    from combine_measures(get_measurement_id(1, 'WS3Cup'),
			  get_measurement_id(1, 'WS3CupB'));

create or replace view wfms_ws_max as
  select get_measurement_id(1, 'WS_Max'),
	 measurement_time,
	 greatest(case when not flagged1 then value1
		  else null end,
		  case when not flagged2 then value2
		  else null end) as value,
	 flagged1 and flagged2 as flagged
    from combine_measures(get_measurement_id(1, 'WS3Cup_Max'),
			  get_measurement_id(1, 'WS3CupB_Max'));

/* Combine all processed data. */
CREATE materialized VIEW processed_measurements as
  select * from processed_campbell_wfms
   union
  select * from wfms_no2
   union
  select * from wfms_slp
   union
  select * from wfms_ws
   union
  select * from wfms_ws_max
   union
  select * from processed_campbell_wfml;
create index processed_measurements_idx on processed_measurements(measurement_type_id, measurement_time);

/* Aggregate the processed data by hour using a function from
   flags.sql. */
CREATE materialized VIEW hourly_measurements as
  select measurement_type_id,
	 measurement_time,
	 value,
	 get_hourly_flag(measurement_type_id, value, n_values::int) as flag
    from (select measurement_type_id,
		 date_trunc('hour', measurement_time) as measurement_time,
		 case when (select measurement like '%\_Max'
			      from measurement_types
			     where id=measurement_type_id)
		   then max(value) FILTER (WHERE not flagged)
		 else avg(value) FILTER (WHERE not flagged) end as value,
		 count(value) FILTER (WHERE not flagged) as n_values
	    from processed_measurements
	   group by measurement_type_id, date_trunc('hour', measurement_time)) c1;
create index hourly_measurements_idx on hourly_measurements(measurement_type_id, measurement_time);

/* Update the processed data. */
CREATE OR REPLACE FUNCTION update_processing(text)
  RETURNS void as $$
  begin
    EXECUTE format('refresh materialized view %I',
		   'processed_' || $1);
  end;
$$ language plpgsql;

CREATE OR REPLACE FUNCTION update_all(text)
  RETURNS void as $$
  declare
  data_sources text[] := array['campbell_wfms', 'campbell_wfml'];
  begin
    refresh materialized view calibration_values;
    refresh materialized view conversion_efficiencies;
    refresh materialized view freezing_clusters;
    if $1='all' then
      for i in 1..array_upper(data_sources, 1) loop
	perform update_processing(data_sources[i]);
      end loop;
    else
      perform update_processing($1);
    end if;
    refresh materialized view processed_measurements;
    refresh materialized view hourly_measurements;
  end;
$$ language plpgsql;
