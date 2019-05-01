/* Instrument calibration */

create table autocals (
  measurement_type_id int references measurement_types,
  type text,
  dates daterange,
  times timerange,
  value numeric default 0,
  primary key(measurement_type_id, dates, times),
  CONSTRAINT no_overlapping_autocals EXCLUDE USING GIST (
    measurement_type_id WITH =,
    dates WITH &&,
    times WITH &&
  )
);

create table manual_calibrations (
  measurement_type_id int references measurement_types,
  type text,
  cal_time timestamp,
  provided_value numeric,
  measured_value numeric,
  corrected boolean not null,
  primary key(measurement_type_id, type, cal_time)
);

create or replace function estimate_cal(measurement_type int, cal_type text, cal_times tsrange) returns numeric as $$
select case when cal_type='zero' then min(moving_average)
       else max(moving_average) end
  from (select instrument_time,
	       AVG(value) OVER(partition by measurement_type_id
			       ORDER BY instrument_time
			       ROWS BETWEEN 1 PRECEDING AND 1 following) as moving_average
	  from measurements2
	 where measurement_type_id=$1
	   and instrument_time between (lower(cal_times) - interval '2 minutes') and (upper(cal_times) + interval '2 minutes')) moving_averages
  -- ignore first 15 minutes of span calibration data due to spikes I
  -- think?
  where instrument_time <@ case when cal_type='zero' then cal_times
			   else tsrange(lower(cal_times) + interval '15 minutes', upper(cal_times)) end;
$$ language sql STABLE PARALLEL SAFE;

create or replace view calibration_periods as
  select measurement_type_id,
	 type,
	 tsrange(cal_day + lower(cal_times),
		 cal_day + upper(cal_times),
		 '[]') as cal_times
    from (select measurement_type_id,
		 type,
		 generate_series(lower(dates),
				 coalesce(upper(dates), CURRENT_DATE),
				 interval '1 day')::date as cal_day,
		 times as cal_times
	    from autocals) a1;

CREATE or replace VIEW calibration_zeros AS
  select *
    from (select *,
		 estimate_cal(measurement_type_id, type, cal_times) as value
	    from calibration_periods
	   where type = 'zero') c1
   where value is not null
  union
  select measurement_type_id,
	 type,
	 tsrange(cal_time - interval '1 hour',
		 cal_time, '[]') as cal_times,
	 measured_value as value
    from manual_calibrations m1
	   join measurement_types m2
	       on m1.measurement_type_id=m2.id
     and type = 'zero';

CREATE OR REPLACE FUNCTION interpolate_cal_zero(measurement_type_id int, t timestamp)
  RETURNS numeric AS $$
  select interpolate(t0, t1, y0, y1, $2)
    from (select upper(cal_times) as t0,
		 value as y0
	    from calibration_zeros
	   where upper(cal_times)<=$2
	     and measurement_type_id=$1
	     and value is not null
	   order by upper(cal_times) desc
	   limit 1) calib0
	 full outer join
	 (select upper(cal_times) as t1,
		 value as y1
	    from calibration_zeros
	   where upper(cal_times)>$2
	     and measurement_type_id=$1
	     and value is not null
	   order by upper(cal_times) asc
	   limit 1) calib1
         on true;
$$ LANGUAGE sql STABLE PARALLEL SAFE;

CREATE or replace VIEW calibration_spans AS
  select c1.*,
	 m.span as provided_value
    from (select *,
		 estimate_cal(measurement_type_id, type, cal_times) as value
	    from calibration_periods
	   where type = 'span') c1
	   join
	   measurement_types m
	   on c1.measurement_type_id=m.id
   where value is not null
  union
  select measurement_type_id,
	 type,
	 tsrange(cal_time - interval '1 hour',
		 cal_time, '[]') as cal_times,
	 provided_value,
	 measured_value as value
    from manual_calibrations m1
	   join measurement_types m2
	       on m1.measurement_type_id=m2.id
     and type = 'span';

CREATE OR REPLACE FUNCTION calc_span(measurement_type_id int, provided_val numeric, val numeric, ts timestamp)
  RETURNS numeric AS $$
  select (val - interpolate_cal_zero(measurement_type_id, ts)) / provided_val;
$$ LANGUAGE sql STABLE PARALLEL SAFE;

/* Store calibration estimates derived from the raw instrument values
 */
CREATE MATERIALIZED VIEW calibration_values AS
  select measurement_type_id,
	 type,
	 cal_times,
	 value
    from calibration_zeros
   union
  select measurement_type_id,
	 type,
	 cal_times,
	 calc_span(measurement_type_id, provided_value,
		   value, upper(cal_times)) as value
    from calibration_spans;
-- to make the interpolate_cal function faster
CREATE INDEX calibration_values_upper_time_idx ON calibration_values(measurement_type_id, type, upper(cal_times));

/* Estimate calibration values using linear interpolation */
CREATE OR REPLACE FUNCTION interpolate_cal(measurement_type_id int, type text, t timestamp)
  RETURNS numeric AS $$
  select interpolate(t0, t1, y0, y1, $3)
    from (select upper(cal_times) as t0,
		 value as y0
	    from calibration_values
	   where upper(cal_times)<=$3
	     and measurement_type_id=$1
	     and type=$2
	     and value is not null
	   order by upper(cal_times) desc
	   limit 1) calib0
	 full outer join
	 (select upper(cal_times) as t1,
		 value as y1
	    from calibration_values
	   where upper(cal_times)>$3
	     and measurement_type_id=$1
	     and type=$2
	     and value is not null
	   order by upper(cal_times) asc
	   limit 1) calib1
         on true;
$$ LANGUAGE sql STABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION apply_calib(measurement_type_id int, val numeric, t timestamp)
  RETURNS numeric AS $$
  DECLARE
  zero numeric = interpolate_cal(measurement_type_id, 'zero', t);
  span numeric = interpolate_cal(measurement_type_id, 'span', t);
  BEGIN
    return case when span is null then val - zero
      else (val - zero) / span end;
  END;
$$ LANGUAGE plpgsql STABLE PARALLEL SAFE;
-- for whatever reason this is faster as a plpgsql function

-- CREATE OR REPLACE FUNCTION apply_calib(site_id int, measurement text, val numeric, t timestamp)
--   RETURNS numeric AS $$
--   select case when span is null then val - zero
-- 	 else (val - zero) / (span - zero) *
-- 	   (select m.span
-- 	      from measurements m
-- 	     where m.site_id=$1
-- 	       and m.measurement=$2) end
--   from (select interpolate_cal(site_id, measurement, 'zero', t) as zero,
-- 	       interpolate_cal(site_id, measurement, 'span', t) as span) as cals;
-- $$ LANGUAGE sql STABLE PARALLEL SAFE;

CREATE MATERIALIZED VIEW conversion_efficiencies AS
  select *,
	 (median(efficiency) over w)::numeric as filtered_efficiency
    from (select measurement_type_id,
		 cal_times,
		 apply_calib(measurement_type_id, value,
			     upper(cal_times)) / max_ce as efficiency
	    from (select *,
			 estimate_cal(measurement_type_id, type, cal_times) as value
		    from calibration_periods
		   where type='CE') c1
		   join measurement_types m1
		       on c1.measurement_type_id=m1.id
	   where value is not null) ce1
	 window w as (partition by measurement_type_id
		      order by upper(cal_times)
		      rows between 15 preceding and 15 following);
-- to make the interpolate_ce function faster
CREATE INDEX conversion_efficiencies_upper_time_idx ON conversion_efficiencies(upper(cal_times));

/* Estimate conversion efficiencies using linear interpolation */
CREATE OR REPLACE FUNCTION interpolate_ce(measurement_type_id int, t timestamp)
  RETURNS numeric AS $$
  select interpolate(t0, t1, y0, y1, $2)
    from (select upper(cal_times) as t0,
		 filtered_efficiency as y0
	    from conversion_efficiencies
	   where upper(cal_times)<=$2
	     and measurement_type_id=$1
	     and filtered_efficiency is not null
	   order by upper(cal_times) desc
	   limit 1) ce0
         full outer join
         (select upper(cal_times) as t1,
		 filtered_efficiency as y1
	    from conversion_efficiencies
	   where upper(cal_times)>$2
	     and measurement_type_id=$1
	     and filtered_efficiency is not null
	   order by upper(cal_times) asc
	   limit 1) ce1
         on true;
$$ LANGUAGE sql STABLE PARALLEL SAFE;
