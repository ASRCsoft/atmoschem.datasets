#include <postgres.h>
#include <fmgr.h>
#include <math.h>

PG_MODULE_MAGIC;

#include "mediator.c"

typedef struct median_state {
  Mediator *mediator;
  int needs_inv;
} median_state;


Datum median_transfn(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(median_transfn);

Datum
median_transfn(PG_FUNCTION_ARGS)
{
  MemoryContext aggContext;
  median_state *state;
  if (!AggCheckCallContext(fcinfo, &aggContext)) {
    elog(ERROR, "median_transfn called in non-aggregate context");
  }
  if (PG_ARGISNULL(0)) {
    // if the state hasn't been initialized yet
    state = MemoryContextAlloc(aggContext, sizeof(median_state));
    state->mediator = MediatorNew(241);
    state->needs_inv = 0;
  } else {
    state = (median_state *)PG_GETARG_POINTER(0);
  }
  MediatorInsert(state->mediator, PG_GETARG_FLOAT8(1),
		 PG_ARGISNULL(1));

  state->needs_inv = 0;
  PG_RETURN_POINTER(state);
}


Datum median_invtransfn(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(median_invtransfn);

Datum
median_invtransfn(PG_FUNCTION_ARGS)
{
  MemoryContext aggContext;
  median_state *state;
  if (!AggCheckCallContext(fcinfo, &aggContext)) {
    elog(ERROR, "median_invtransfn called in non-aggregate context");
  }
  state = (median_state *)PG_GETARG_POINTER(0);

  // When the window is shifted, invtransfn is called before transfn,
  // but transfn does invtransfn's job in most cases. In the other
  // cases, the finalfn can use this indicator to realize that
  // invtransfn still needs to be run and do that before returning the
  // final value. Also the inverse function only has an effect for
  // non-null values.
  state->needs_inv = !PG_ARGISNULL(1);
  PG_RETURN_POINTER(state);
}

Datum median_finalfn(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(median_finalfn);

Datum
median_finalfn(PG_FUNCTION_ARGS)
{
  MemoryContext aggContext;
  median_state *state;
  if (!AggCheckCallContext(fcinfo, &aggContext)) {
    elog(ERROR, "median_finalfn called in non-aggregate context");
  }
  state = PG_ARGISNULL(0) ? NULL : (median_state *)PG_GETARG_POINTER(0);
  if (state == NULL) PG_RETURN_NULL();

  if (state->needs_inv) {
    // didn't run invtransfn because I was expecting transfn to run
    // right afterward, but apparently that didn't happen. This is
    // probably near the end of the sequence. So we'll just do the
    // invtransfn here.
    MediatorPopOldest(state->mediator);
    state->needs_inv = 0;
  }
  PG_RETURN_FLOAT8(MediatorMedian(state->mediator));
}


/* Getting a running MAD */
typedef struct mad_state {
  Mediator *median_mediator;
  Mediator *mad_mediator;
  float previous_median;
} mad_state;


Datum mad_transfn(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(mad_transfn);

Datum
mad_transfn(PG_FUNCTION_ARGS)
{
  MemoryContext aggContext;
  mad_state *state;
  float new_median;
  float abs_dev;
  int i;
  int p_i;
  int isnull;
  if (!AggCheckCallContext(fcinfo, &aggContext)) {
    elog(ERROR, "mad_transfn called in non-aggregate context");
  }
  if (PG_ARGISNULL(0)) {
    // if the state hasn't been initialized yet
    state = MemoryContextAlloc(aggContext, sizeof(mad_state));
    state->median_mediator = MediatorNew(241);
    state->mad_mediator = MediatorNew(241);
  } else {
    state = (mad_state *)PG_GETARG_POINTER(0);
  }
  MediatorInsert(state->median_mediator, PG_GETARG_FLOAT8(1),
		 PG_ARGISNULL(1));
  new_median = MediatorMedian(state->median_mediator);

  if ((!PG_ARGISNULL(0)) &&
      new_median == state->previous_median) {
    /* update the running MAD */
    abs_dev = fabs(PG_GETARG_FLOAT8(1) - new_median);
    MediatorInsert(state->mad_mediator, abs_dev,
		   PG_ARGISNULL(1));
  } else {
    /* restart MAD calculations */
    free(state->mad_mediator);
    state->mad_mediator = MediatorNew(241);
    /* refill with raw values which are stored in the
       median_mediator */
    for (i=0; i<241; i++) {
      // check that the value is not null
      p_i = state->median_mediator->pos[i];
      isnull = MediatorPosIsNull(state->median_mediator, p_i);
      abs_dev = fabs(state->median_mediator->data[i] -
  		     new_median);
      MediatorInsert(state->mad_mediator, abs_dev, isnull);
    }
  }
  state->previous_median = new_median;
  PG_RETURN_POINTER(state);
}


Datum mad_invtransfn(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(mad_invtransfn);

Datum
mad_invtransfn(PG_FUNCTION_ARGS)
{
  MemoryContext aggContext;
  mad_state *state;
  if (!AggCheckCallContext(fcinfo, &aggContext)) {
    elog(ERROR, "mad_invtransfn called in non-aggregate context");
  }
  state = (mad_state *)PG_GETARG_POINTER(0);
  PG_RETURN_POINTER(state);
}


Datum mad_finalfn(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(mad_finalfn);

Datum
mad_finalfn(PG_FUNCTION_ARGS)
{
  MemoryContext aggContext;
  mad_state *state;
  if (!AggCheckCallContext(fcinfo, &aggContext)) {
    elog(ERROR, "mad_finalfn called in non-aggregate context");
  }
  state = PG_ARGISNULL(0) ? NULL : (mad_state *)PG_GETARG_POINTER(0);
  if (state == NULL) PG_RETURN_NULL();
  PG_RETURN_FLOAT8(MediatorMedian(state->mad_mediator));
}
