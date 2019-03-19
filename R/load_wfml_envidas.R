#!/usr/bin/env Rscript
## getting WFML envidas data into postgres

## run this script from the command line like so:
## Rscript load_envidas.R dbname ~/data/envidas/WFML/7-1-2018 - 9-30-2018.csv

library(RPostgreSQL)
library(dbx)
library(tidyr)

update_measurement_types = function(site_id, df) {
  ## add measurement types that don't already exist in postgres
  mt_df = data.frame(site_id = site_id,
                     measurement = unique(df$measurement),
                     data_source = 'envidas')
  pg = dbxConnect(adapter = 'postgres', dbname = dbname)
  dbxUpsert(pg, 'measurement_types', mt_df,
            where_cols = c('site_id', 'measurement', 'data_source'),
            skip_existing = T)
  dbxDisconnect(pg)
}

get_measurement_types = function() {
  pg = dbxConnect(adapter = 'postgres', dbname = dbname)
  measurement_types = dbxSelect(pg, "select * from measurement_types where site_id=2 and data_source='envidas'")
  dbxDisconnect(pg)
  measurement_types
}

read_envidas = function(f, ...) {
  na_strings = c('NA', 'NAN', 'NANN', '-9999')
  df = read.csv(f, header = T, na.strings = na_strings,
                skip = 1, check.names = F,
                fileEncoding = 'UTF-8',
                stringsAsFactors = F, ...)
  df
}

write_envidas = function(f) {
  df = read_envidas(f)
  ## get timestamps from date and time columns
  df$instrument_time = as.POSIXct(paste(df$Date, df$Time),
                                  format = '%m/%d/%Y %I:%M %p',
                                  tz = 'UTC')
  ## ^ it's not really UTC but this keeps R from mucking with time
  ## zones

  ## get data frame of values
  df_vals = df[, grepl(' : Value|instrument_time', names(df))]
  names(df_vals) = gsub(' : Value', '', names(df_vals))
  long_vals = gather(df_vals, measurement, value, -instrument_time)

  ## get data frame of flags
  df_flags = df[, grepl(' : Status|instrument_time', names(df))]
  names(df_flags) = gsub(' : Status', '', names(df_flags))
  long_flags = gather(df_flags, measurement, flag,
                      -instrument_time)

  long_df = merge(long_vals, long_flags)
  long_df$flagged = long_df$flag != 1
  long_df$flag = NULL

  ## organize the measurement type IDs
  update_measurement_types(2, long_df)
  measurement_types = get_measurement_types()
  long_df$measurement_type_id =
    measurement_types$id[match(long_df$measurement,
                               measurement_types$measurement)]
  long_df$measurement = NULL
  
  pg = dbConnect(PostgreSQL(), dbname = dbname)
  dbWriteTable(pg, 'measurements', long_df, append = T,
               row.names = F)
  dbDisconnect(pg)
}

dbname = commandArgs(trailingOnly = T)[1]
env_files = commandArgs(trailingOnly = T)[-1]
for (f in env_files) {
  message(paste('Importing', f))
  write_envidas(f)
}
