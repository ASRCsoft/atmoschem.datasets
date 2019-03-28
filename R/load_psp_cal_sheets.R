#!/usr/bin/env Rscript
## get manual calibrations from pdf cal sheets

## run this script from the command line like so:
## Rscript load_psp_cal_sheets.R dbname ~/data/calibrations/PSP/Pinnacle_42C_NOX 181025.xls

library(readxl)
library(dbx)

dbname = commandArgs(trailingOnly = T)[1]

## get the sites and corresponding IDs
pg = dbxConnect(adapter = 'postgres', dbname = dbname)
sites = dbxSelect(pg, 'select * from sites')
measurement_types = dbxSelect(pg, 'select * from measurement_types where site_id=3')
dbxDisconnect(pg)

get_measurement_type_id = function(m)
  measurement_types$id[match(m, measurement_types$measurement)]

write_cal = function(site_id, measure, cal_type, cal_time,
                     measured_value, corrected) {
  measurement_type_id = get_measurement_type_id(measure)
  df = data.frame(measurement_type_id = measurement_type_id,
                  type = cal_type,
                  cal_time = cal_time,
                  measured_value = measured_value,
                  corrected = corrected)
  idx_cols = c('measurement_type_id', 'type', 'cal_time')
  pg = dbxConnect(adapter = 'postgres', dbname = dbname)
  dbxUpsert(pg, 'manual_calibrations', df, where_cols = idx_cols)
  dbxDisconnect(pg)
}

import_42C = function(f) {
  ## need to get "zero and span checks" section
  df1 = read_xls(f, range = 'F33:AD35', col_names = F)
  cals = as.data.frame(df1[c(1, 3), c(1, 8, 14)])
  names(cals) = c('cert_span', 'zero', 'span')
  row.names(cals) = c('NO', 'NOx')
  
  ## get times from the header section
  df2 = as.data.frame(read_xls(f, range = 'K10:AF10', col_names = F))
  cal_date = df2[1, 1]
  start_time = df2[1, 16]
  end_time = df2[1, 22]
  ## occasionally a formatting error (I think?) causes us to get the
  ## time as a string
  if (class(start_time) == 'character') {
    cal_start = as.POSIXct(paste(format(cal_date, '%F'), start_time, '%T'))
  } else {
    cal_start = as.POSIXct(paste(format(cal_date, '%F'), format(start_time, '%T')))
  }
  ## errors with end time render the calibration unusable
  if (is.na(end_time)) {
    warning(paste('Unable to retrieve calibration time from file', f))
    return(NULL)
  }
  cal_end = as.POSIXct(paste(format(cal_date, '%F'), format(end_time, '%T')))

  for (chem in c('NO', 'NOx'))
    for (type in c('zero', 'span'))
      write_cal(3, chem, type, cal_end, cals[chem, type], FALSE)
}

files = commandArgs(trailingOnly = T)[-1]
for (f in files) {
  message(paste('Importing', f))
  file_type = gsub('^.*/Pinnacle_|_[^_]*$', '', f)
  if (file_type == '42C') {
    import_42C(f)
  }
}



## ## testing
## f = '/home/wmay/data/calibrations/PSP/Pinnacle_42C_NOX 181025.xls'
## f = '/home/wmay/data/calibrations/PSP/Pinnacle_42C_NOX 181015.xls'
## f = '/home/wmay/data/calibrations/PSP/Pinnacle_42C_NOX 180724.xls'
## x1 = read_xls(f)
## read_42C(f)
## ## need to get "zero and span checks" and header section with times
## x1 = read_xls(f, range = 'F33:AD35', col_names = F)
## x2 = as.data.frame(x1[c(1, 3), c(1, 8, 14)])
## names(x2) = c('cert_span', 'zero', 'span')
## row.names(x2) = c('NO', 'NOx')
