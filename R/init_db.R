#!/usr/bin/env Rscript
## Set up the postgres database

## run this script from the command line like so:
## Rscript init_db.R dbname

library(RPostgreSQL)

## set up database connection
dbname = commandArgs(trailingOnly = T)[1]
pg = dbConnect(PostgreSQL(), dbname = dbname)

## add to postgres' libdir so it can find compiled code
## pg_src_path = system.file('libs',
##                           package = 'nysatmoschem')
pg_src_path = normalizePath(file.path('..', 'src'))
## (think I need to change the colon to semi-colon on windows)
dynamic_library_path = paste(pg_src_path, '$libdir', sep = ':')
sql_txt = paste0("set dynamic_library_path to '",
                 dynamic_library_path, "'")
dbClearResult(dbSendStatement(pg, sql_txt))

## set up tables and functions
sql_files = c('utilities', 'setup', 'filtering',
              'calibration', 'flags', 'processing')
for (sql_file in sql_files) {
  message(paste('starting', sql_file))
  ## this works a lot better than dbRunScript in the default
  ## method
  ## schema = find_schema(obj, sql_file, 'nysatmoschem', 'sql')
  schema = file.path('..', 'postgres', paste(sql_file, 'sql', sep = '.'))
  sql_txt = paste(readLines(schema), collapse = "\n")
  suppressWarnings(dbClearResult(dbSendStatement(pg, sql_txt)))
}

dbDisconnect(pg)
