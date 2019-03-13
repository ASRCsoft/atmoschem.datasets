#!/usr/bin/env Rscript
## getting campbell data into postgres

## run this script from the command line like so:
## Rscript load_campbell.R dbname ~/data/campbell/WFMS/WFMFS_SUMMIT_Table1_2018_06_29_2300.dat

library(parallel)
library(dbx)
library(tidyr)

wfms_flags = c(NO = 'NOX', NOx = 'NOX', T = 'TRH',
               RH = 'TRH', NOy = 'NOY', SO2 = 'SO2',
               CO = 'CO')
wfml_flags = c(CO = 'CO', NO = 'NOX', NO2 = 'NOX')

dbname = commandArgs(trailingOnly = T)[1]

## get the sites and corresponding IDs
pg = dbxConnect(adapter = 'postgres', dbname = dbname)
sites = dbxSelect(pg, 'select * from sites')
dbxDisconnect(pg)

update_measurement_types = function(site_id, df) {
  ## add measurement types that don't already exist in postgres
  mt_df = data.frame(site_id = site_id,
                     measurement = unique(df$measurement),
                     data_source = 'campbell')
  pg = dbxConnect(adapter = 'postgres', dbname = dbname)
  dbxUpsert(pg, 'measurement_types', mt_df,
            where_cols = c('site_id', 'measurement', 'data_source'),
            skip_existing = T)
  dbxDisconnect(pg)
}

get_measurement_types = function() {
  pg = dbxConnect(adapter = 'postgres', dbname = dbname)
  measurement_types = dbxSelect(pg, "select * from measurement_types where data_source='campbell'")
  dbxDisconnect(pg)
  measurement_types
}

fast_lookup = function(vals, dict) {
  ## This code to get dictionary (named vector) entries shouldn't need
  ## to be this long but the current version of R seems to have a very
  ## slow lookup function for named vectors, so we need extra code to
  ## work around that.
  res = rep(NA, length(vals))
  has_entry = vals %in% names(dict)
  res[has_entry] = dict[vals[has_entry]]
  res
}

read_campbell = function(f) {
  ## headers are on the second line, but data starts on line 4
  headers = read.csv(f, header=F, skip = 1, nrows=1,
                     stringsAsFactors = F)
  na_strings = c('NAN', 'NaN')
  df = read.csv(f, header=F, skip = 4, col.names = headers,
                na.strings = na_strings, stringsAsFactors = F)
  ## replace 'TIMESTAMP' with 'instrument_time' to match other data
  ## tables
  names(df)[1] = 'instrument_time'
  ## replace 'CupA' with 'Cup' for consistency over time
  names(df) = gsub('CupA', 'Cup', names(df))
  ## remove empty columns
  df[, -grep('Spare|Phase', names(df), ignore.case = T)]
}

fix_wfms = function(df) {
  ## fix various issues with WFMS data
  
  ## replace mislabeled NO2 column
  names(df)[names(df) == 'NO2_Avg'] = 'NOx_Avg'
  ## adjust miscalculated wind speeds
  df$WS3Cup_Avg[df$instrument_time > '2016-12-14' &
                df$instrument_time < '2019-02-14 15:57'] =
    (.5 / .527) * df$WS3Cup_Avg - (.5 / .527) - .5
  df$WS3Cup_Max[df$instrument_time > '2016-12-14' &
                df$instrument_time < '2019-02-14 15:57'] =
    (.5 / .527) * df$WS3Cup_Max - (.5 / .527) - .5
  ## ignore some incorrect flags
  df$F_TRH_Avg[df$instrument_time > '2018-12-01' &
               df$instrument_time < '2019-01-01'] = 1
  ## replace malfunctioning wind direction values with NA
  df[df$WindDir_SD1_WVT == 0,
     c('WindDir_D1_WVT', 'WindDir_SD1_WVT')] = NA
  df
}

write_campbell = function(f) {
  campbell = read_campbell(f)
  path_folders = strsplit(f, '/')[[1]]
  ## campbell$file = tail(path_folders, 1)
  ## get the site
  site = path_folders[length(path_folders) - 1]
  if (site == 'WFMS') {
    campbell = fix_wfms(campbell)
  }
  
  ## clean and reorganize the data
  is_flag = grepl('^F_', names(campbell))
  campbell_long = campbell[, !is_flag] %>%
    gather(measurement, value, -c(instrument_time, RECORD))
  if (site == 'WFMS') {
    flag_mat = as.matrix(campbell[, is_flag]) != 1
  } else if (site == 'WFML') {
    flag_mat = as.matrix(campbell[, is_flag]) != 0
  }
  row.names(flag_mat) = campbell$instrument_time
  campbell_long$measurement =
    gsub('_Avg$', '', campbell_long$measurement)
  flag_rows = match(campbell_long$instrument_time,
                    campbell$instrument_time)
  flag_cols = match(paste('F',
                          fast_lookup(campbell_long$measurement,
                                      wfms_flags),
                          'Avg', sep = '_'),
                    colnames(flag_mat))
  campbell_long$flagged = flag_mat[cbind(flag_rows, flag_cols)]
  
  ## add to postgres
  names(campbell_long) = tolower(names(campbell_long))
  site_id = sites$id[sites$short_name == site]
  update_measurement_types(site_id, campbell_long)
  measurement_types = get_measurement_types()
  site_measurement_types =
    measurement_types[measurement_types$site_id == site_id, ]
  campbell_long$measurement_type_id =
    site_measurement_types$id[match(campbell_long$measurement,
                                    site_measurement_types$measurement)]
  campbell_long$measurement = NULL
  pg = dbxConnect(adapter = 'postgres', dbname = dbname)
  ## Some files contain duplicate times due to a datalogger
  ## restart. However, all the duplicated times appear to have
  ## matching values, so we can skip updates for those pre-existing
  ## rows.

  ## It would be prudent to add a check here, though, to verify that
  ## duplicated times have matching values.
  dbxUpsert(pg, 'measurements', campbell_long,
            where_cols = c('instrument_time',
                           'measurement_type_id'),
            batch_size = 10000, skip_existing = T)
  dbxDisconnect(pg)
}

files = commandArgs(trailingOnly = T)[-1]
file_date_strs = gsub('^.*Table1_|\\.dat$', '', files)
files = files[order(file_date_strs)]
n_cores = max(1, detectCores() - 1)
mclapply(files, write_campbell, mc.cores = n_cores)
