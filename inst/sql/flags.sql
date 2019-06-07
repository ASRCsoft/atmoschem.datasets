/* Determing flag values. */

create table manual_flags (
  measurement_type_id int references measurement_types,
  times tsrange,
  aqs_flag text not null,
  explanation text,
  primary key(measurement_type_id, times),
  CONSTRAINT no_duplicated_flags EXCLUDE USING GIST (
    measurement_type_id WITH =,
    explanation WITH =,
    times WITH &&
  )
);

/* Find frozen wind periods */
-- get the anemometer measurement type IDs
create or replace function anemometer_ids() RETURNS int[] AS $$
  select array_agg(id)
    from (select id
	    from measurement_types
	   where name in ('WS3Cup',
			  'WS3Cup_Max',
			  'WS3CupB',
			  'WS3CupB_Max')) m1;
$$ LANGUAGE sql stable parallel safe;

-- see if the anemometers look frozen
CREATE or replace VIEW wind_looks_frozen AS
  select measurement_type_id,
	 time,
	 (bit_and(slow_wind::int) over w)::boolean as looks_frozen
    from (select measurement_type_id,
		 time,
		 value < .2 as slow_wind
	    from measurements2
	   where measurement_type_id=any(anemometer_ids())) w1
	   window w as (partition by measurement_type_id
			order by time
			rows between current row and 30 following);

-- get contiguous periods of frozen anemometer times
CREATE or replace VIEW contiguous_freezes AS
  select measurement_type_id,
	 tsrange(min(time) - interval '1 hour',
		 max(time) + interval '40 minutes') as freezing_times
    from (select *,
		 sum(freeze_starts::int) over w as freeze_group
	    from (select *,
			 looks_frozen and not lag(looks_frozen) over w as freeze_starts
		    from wind_looks_frozen
			   window w as (partition by measurement_type_id
					order by time)) w1
		   window w as (partition by measurement_type_id
				order by time)) w2
   where looks_frozen
   group by measurement_type_id, freeze_group;

-- filter contiguous_freezes using the corresponding site temperature
-- at the beginning of the freeze
CREATE or replace VIEW contiguous_cold_freezes AS
  select f1.measurement_type_id,
	 freezing_times
    from contiguous_freezes f1
  	   join measurement_types mt1
	       on f1.measurement_type_id=mt1.id
	   join measurement_types mt2
	       on mt1.data_source_id=mt2.data_source_id
	   join measurements2 m1
	       on m1.measurement_type_id=mt2.id
	       and m1.time=(lower(freezing_times) + interval '1 hour')
   where mt2.name='T'
     and m1.value<5;

-- get clusters of frozen anemometer times
CREATE materialized VIEW freezing_clusters AS
  select measurement_type_id,
	 tsrange(min(lower(freezing_times)),
		 max(upper(freezing_times))) as freeze_period
    from (select *,
		 sum(new_freeze::int) over w as freeze_cluster
	    from (select *,
			 coalesce(not freezing_times && lag(freezing_times) over w,
				  true) as new_freeze
		    from contiguous_cold_freezes
			   window w as (partition by measurement_type_id
					order by lower(freezing_times))) w1
		   window w as (partition by measurement_type_id
				order by lower(freezing_times))) w2
   group by measurement_type_id, freeze_cluster;
CREATE INDEX freezing_clusters_idx ON freezing_clusters using gist(measurement_type_id, freeze_period);

/* Check for flags */
create or replace function has_manual_flag(measurement_type int, measurement_time timestamp) RETURNS bool AS $$
  select exists(select *
		  from manual_flags
		 where measurement_type_id=$1
		   and $2 <@ times);
$$ LANGUAGE sql stable parallel safe;

create or replace function has_calibration_flag(measurement_type_id int, measurement_time timestamp) RETURNS bool AS $$
  -- check to see if a measurement occurred in a calibration period,
  -- or immediately after one
  -- (need to add a check for manual calibrations here)
  select exists(select *
		  from autocals
		 where measurement_type_id=$1
		   and $2::date <@ dates
		   and $2::time <@ timerange(lower(times),
					     upper(times) + interval '5 minutes'));
$$ LANGUAGE sql stable parallel safe;

create or replace function is_valid_value(measurement_type_id int, value numeric) RETURNS bool AS $$
  select coalesce(value <@ (select valid_range
			      from measurement_types
			     where id=$1), true);
$$ LANGUAGE sql stable parallel safe;

create or replace function is_outlier(value numeric, median double precision, mad double precision) RETURNS bool AS $$
  -- Decide if a value is an outlier using the method from the Hampel
  -- filter. The term (1.4826 * MAD) estimates the standard
  -- deviation. See
  -- https://en.wikipedia.org/wiki/Median_absolute_deviation#Relation_to_standard_deviation
  select abs(value - median) / (1.4826 * nullif(mad, 0)) > 3.5;
$$ LANGUAGE sql immutable RETURNS NULL ON NULL INPUT parallel safe;

create or replace function is_below_mdl(measurement_type_id int, value numeric) RETURNS bool AS $$
  select coalesce(value < (select mdl
			     from measurement_types
			    where id=$1), false);
$$ LANGUAGE sql stable parallel safe;

create or replace function is_frozen(measurement_type_id int, measurement_time timestamp) RETURNS bool AS $$
  select exists(select *
		  from freezing_clusters
		 where measurement_type_id=$1
		   and $2 <@ freeze_period);
$$ LANGUAGE sql stable parallel safe;
  
/* Determine if a measurement is flagged. */
create or replace function is_flagged(measurement_type_id int, source sourcerow, measurement_time timestamp, value numeric, flagged boolean, median double precision, mad double precision) RETURNS bool AS $$
  -- Check for: 1) manual flags, 2) instrument flags, 3) calibrations,
  -- 4) invalid values, 5) outliers (based on the Hampel filter), and
  -- 6) freezing anemometers
  select has_manual_flag(measurement_type_id, measurement_time)
	   or coalesce(flagged, false)
	   or has_calibration_flag(measurement_type_id, measurement_time)
	   or not is_valid_value(measurement_type_id, value)
	   or coalesce(is_outlier(value, median, mad), false)
	   or case when measurement_type_id=any(anemometer_ids()) then is_frozen(measurement_type_id, measurement_time)
	      else false end;
$$ LANGUAGE sql stable parallel safe;

/* Get the NARSTO averaged data flag based on the number of
measurements and average value. */
CREATE OR REPLACE FUNCTION get_hourly_flag(measurement_type_id int, value numeric, n int) RETURNS text AS $$
  SELECT case when n<30 then 'M1'
	 when is_below_mdl(measurement_type_id, value) then 'V1'
	 when n<45 then 'V4'
	 else 'V0' end;
$$ LANGUAGE sql immutable parallel safe;
