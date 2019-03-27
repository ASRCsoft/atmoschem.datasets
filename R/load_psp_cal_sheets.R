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
measurement_types = dbxSelect(pg, 'select * from measurement_types')
dbxDisconnect(pg)

get_measurement_type_id = function(site_id, m) {
  measurement_types[measurement_types$site_id == site_id &
                    measurement_types$measurement == m, 'id']
}

write_cal = function(site, measure, cal_type, cal_time,
                     measured_value, corrected) {
  site_id = sites$id[match(site, sites$short_name)]
  if (site_id == 2 & measure == 'NOx') {
    ## capitalize the 'x' for now to match the WFML Campbell files
    measure = 'NOX'
  }
  measurement_type_id = get_measurement_type_id(site_id, measure)
  df = data.frame(measurement_type_id = measurement_type_id,
                  type = cal_type,
                  cal_time = cal_time,
                  measured_value = measured_value,
                  corrected = corrected)
  idx_cols = c('measurement_type_id', 'cal_time')
  pg = dbxConnect(adapter = 'postgres', dbname = dbname)
  dbxUpsert(pg, 'manual_calibrations', df, where_cols = idx_cols)
  dbxDisconnect(pg)
}

read_42C = function(f) {
  ## need to get "zero and span checks" and 
  df1 = read_xls(f, range = 'F33:AD35', col_names = F)
  cals = as.data.frame(x1[c(1, 3), c(1, 8, 14)])
  names(cals) = c('cert_span', 'zero', 'span')
  row.names(cals) = c('NO', 'NOx')
  
  ## get times from the header section
  ## ...
  
  ## return a list of calibration results and calibration time bounds
  list(cals = cals)
}

write_42C = function(f) {
  pdf = read_pdf_form(f)
  write_cal(site, 'NO', 'zero check', cal_time,
            pdf$`42ctls_zero_noy_a_7`, FALSE)
  write_cal(site, 'NOx', 'zero check', cal_time,
            pdf$`42ctls_zero_noy_b_7`, FALSE)
}

files = commandArgs(trailingOnly = T)[-1]
for (f in files) {
  message(paste('Importing', f))
  file_type = gsub('^.*/|_[^/]*$', '', f)
  if (file_type == '42C' || file_type == '42i') {
    ## 42C and 42i both use the same form template
    write_42C(f)
  } else if (file_type == '42Cs') {
    write_42Cs(f)
  } else if (file_type == '48C') {
    write_48C(f)
  }
}



## ## testing
## f = '/home/wmay/data/calibrations/PSP/Pinnacle_42C_NOX 181025.xls'
## x1 = read_xls(f)
## ## need to get "zero and span checks" and header section with times
## x1 = read_xls(f, range = 'F33:AD35', col_names = F)
## x2 = as.data.frame(x1[c(1, 3), c(1, 8, 14)])
## names(x2) = c('cert_span', 'zero', 'span')
## row.names(x2) = c('NO', 'NOx')
