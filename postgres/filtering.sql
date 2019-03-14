/* Tables and functions for applying various filters to instrument
   data. */

/* Efficient median filter */
CREATE OR REPLACE FUNCTION runmed_transfn(internal, double precision, int)
RETURNS internal
AS 'median', 'median_transfn'
LANGUAGE c IMMUTABLE parallel safe;

CREATE OR REPLACE FUNCTION runmed_invtransfn(internal, double precision, int)
  RETURNS internal
AS 'median', 'median_invtransfn'
LANGUAGE c IMMUTABLE parallel safe;

CREATE OR REPLACE FUNCTION runmed_finalfn(internal)
RETURNS double precision
AS 'median', 'median_finalfn'
LANGUAGE c IMMUTABLE parallel safe;

CREATE AGGREGATE runmed(double precision, int) (
  stype = internal,
  sfunc = runmed_transfn,
  finalfunc = runmed_finalfn,
  mstype = internal,
  msfunc = runmed_transfn,
  minvfunc = runmed_invtransfn,
  mfinalfunc = runmed_finalfn,
  PARALLEL = SAFE
);

/* Efficient running MAD */
CREATE OR REPLACE FUNCTION runmad_transfn(internal, double precision)
RETURNS internal
AS 'median', 'mad_transfn'
LANGUAGE c IMMUTABLE parallel safe;

CREATE OR REPLACE FUNCTION runmad_invtransfn(internal, double precision)
  RETURNS internal
AS 'median', 'mad_invtransfn'
LANGUAGE c IMMUTABLE parallel safe;

CREATE OR REPLACE FUNCTION runmad_finalfn(internal)
RETURNS double precision
AS 'median', 'mad_finalfn'
LANGUAGE c IMMUTABLE parallel safe;

CREATE AGGREGATE runmad(double precision) (
  stype = internal,
  sfunc = runmad_transfn,
  finalfunc = runmad_finalfn,
  mstype = internal,
  msfunc = runmad_transfn,
  minvfunc = runmad_invtransfn,
  mfinalfunc = runmad_finalfn,
  PARALLEL = SAFE
);
