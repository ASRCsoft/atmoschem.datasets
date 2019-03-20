#!/usr/bin/env Rscript
## getting PSP envidas data into postgres

## run this script from the command line like so:
## Rscript load_envidas.R dbname ~/data/envidas/WFMS/envi-rpt-1101.csv

library(RPostgreSQL)
library(dbx)
library(tidyr)

update_measurement_types = function(df) {
  ## add measurement types that don't already exist in postgres
  mt_df = data.frame(site_id = 3,
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
  measurement_types = dbxSelect(pg, "select * from measurement_types where site_id=3 and data_source='envidas'")
  dbxDisconnect(pg)
  measurement_types
}

read_envidas = function(f, ...) {
  ## get the headers separately since there's a line of units
  ## separating them from the data
  headers = names(read.csv(f, skip = 2, nrows = 1, check.names = F))
  lines = readLines(f, warn = F, encoding = 'UTF-8')
  ## remove extraneous header lines and lines of summary stats at the
  ## bottom of the file
  csv_lines = lines[-c(1:2, 4,
                       (length(lines) - 8):length(lines))]
  csv_text = paste(csv_lines, collapse = '\n')
  na_strings = c('NA', 'NAN', 'NANN', '-9999', '---')
  df = read.csv(text = csv_text, na.strings = na_strings,
                fileEncoding = 'UTF-8', col.names = headers,
                check.names = F, ...)
  df
}

write_envidas = function(f) {
  df = read_envidas(f)
  ## check for newer files with the absurd date format
  file_date = gsub('.*envi_rpt-|\\.csv', '', f)
  if (file_date >= '1809') {
    date_format = '%d/%m/%Y %H:%M'
  } else {
    date_format = '%m/%d/%Y %I:%M %p'
  }
  names(df)[1] = 'instrument_time'
  df$instrument_time = as.POSIXct(df$instrument_time,
                                  format = date_format,
                                  tz = 'UTC')

  ## get data frame of values
  df_vals = df[, !grepl('Status', names(df))]
  long_vals = gather(df_vals, measurement, value,
                     -instrument_time)

  ## get data frame of flags
  df_flags = df[, c(1, grep('Status', names(df)))]
  names(df_flags) = names(df_vals)
  long_flags = gather(df_flags, measurement, flag,
                      -instrument_time)
  long_flags$flagged =
    long_flags$flag == 'OK' | long_flags$flag == ''
  long_flags$flag = NULL

  ## long_df = merge(long_vals, long_flags)
  ## safe to assume both data frames are in the same order for now
  long_df = cbind(long_vals, flagged = long_flags$flagged)

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
env_files = commandArgs(trailingOnly = T)[-1]
for (f in env_files) {
  message(paste('Importing', f))
  write_envidas(f)
}
