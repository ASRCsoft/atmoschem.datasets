#!/usr/bin/env Rscript
## getting mesonet data into postgres

## run this script from the command line like so:
## Rscript load_mesonet.R dbname ~/data/mesonet/WFML/Lodge_MESONET_2018_12.csv

library(RPostgreSQL)
library(dbx)
library(tidyr)

update_measurement_types = function(df) {
  ## add measurement types that don't already exist in postgres
  mt_df = data.frame(site_id = 2,
                     measurement = unique(df$measurement),
                     data_source = 'mesonet')
  pg = dbxConnect(adapter = 'postgres', dbname = dbname)
  dbxUpsert(pg, 'measurement_types', mt_df,
            where_cols = c('site_id', 'measurement', 'data_source'),
            skip_existing = T)
  dbxDisconnect(pg)
}

get_measurement_types = function() {
  pg = dbxConnect(adapter = 'postgres', dbname = dbname)
  measurement_types = dbxSelect(pg, "select * from measurement_types where site_id=2 and data_source='mesonet'")
  dbxDisconnect(pg)
  measurement_types
}

write_mesonet = function(f) {
  df = read.csv(f, check.names = F)
  df$instrument_time = as.POSIXct(df$datetime,
                                  format = '%Y%m%dT%H%M%S',
                                  tz = 'UTC')

  ## get data frame of values
  df_vals = df[, -(1:2)]
  long_df = gather(df_vals, measurement, value,
                   -instrument_time)
  long_df$flagged = F

  ## organize the measurement type IDs
  update_measurement_types(long_df)
  measurement_types = get_measurement_types()
  long_df$measurement_type_id =
    measurement_types$id[match(long_df$measurement,
                               measurement_types$measurement)]
  long_df$measurement = NULL
  
  pg = dbConnect(PostgreSQL(), dbname = dbname)
  dbWriteTable(pg, 'measurements', long_df,
               append = T, row.names = F)
  dbDisconnect(pg)
}

dbname = commandArgs(trailingOnly = T)[1]
meso_files = commandArgs(trailingOnly = T)[-1]
for (f in meso_files) {
  message(paste('Importing', f))
  write_mesonet(f)
}
