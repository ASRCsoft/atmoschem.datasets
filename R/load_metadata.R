#!/usr/bin/env Rscript
## load metadata into postgres

## run this script from the command line like so:
## Rscript load_metadata.R dbname /path/to/metadata/files

library(dbx)

dbname = commandArgs(trailingOnly = T)[1]

## get the sites and corresponding IDs
pg = dbxConnect(adapter = 'postgres', dbname = dbname)
sites = dbxSelect(pg, 'select * from sites')
dbxDisconnect(pg)

get_site_id = function(x) sites$id[match(x, sites$short_name)]

write_metadata = function(f, tbl_name) {
  ## upsert metadata from a metadata csv file
  meta = read.csv(f, na.strings=c('', 'NA'))
  meta$site_id = get_site_id(meta$site)
  meta$site = NULL
  ## get the measurement type IDs
  meta = merge(meta, measurement_types)
  meta$site_id = NULL
  meta$data_source = NULL
  meta$measurement = NULL
  pg = dbxConnect(adapter = 'postgres', dbname = dbname)
  ## clear old data first
  dbxDelete(pg, tbl_name)
  ## now add new data
  dbxInsert(pg, tbl_name, meta)
  dbxDisconnect(pg)
}

write_measurement_types = function(f) {
  measurements_file = file.path(f, 'measurement_types.csv')
  idx_cols = c('data_source', 'measurement', 'site_id')
  ## write_metadata(measurements_file,
  ##                'measurement_types', idx_cols)
  meta = read.csv(measurements_file, na.strings=c('', 'NA'))
  meta$site_id = get_site_id(meta$site)
  meta$site = NULL
  pg = dbxConnect(adapter = 'postgres', dbname = dbname)
  dbxUpsert(pg, 'measurement_types', meta, where_cols = idx_cols)
  dbxDisconnect(pg)
}

write_autocals = function(f) {
  autocal_file = file.path(f, 'autocals.csv')
  write_metadata(autocal_file, 'autocals')
}

write_manual_flags = function(f) {
  flags_file = file.path(f, 'manual_flags.csv')
  write_metadata(flags_file, 'manual_flags')
}


f = commandArgs(trailingOnly = T)[-1]
message('Loading measurements info...')
write_measurement_types(f)
## get the new measurement types
pg = dbxConnect(adapter = 'postgres', dbname = dbname)
measurement_types =
  dbxSelect(pg, 'select id as measurement_type_id, site_id, data_source, measurement from measurement_types')
dbxDisconnect(pg)
## return to loading the metadata
message('Loading autocalibration schedule...')
write_autocals(f)
message('Loading manual flags...')
write_manual_flags(f)
