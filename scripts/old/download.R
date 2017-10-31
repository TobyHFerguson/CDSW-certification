# Make a data directory and use that
dir.create("brfss", recursive=TRUE)
setwd("brfss")

## We will only use years 2011 through 2015 because this is the range
## that is comparable
years <- c(2011:2015)

## Define some functions to convert from a year to the relevant URL or name
get_url <- function(yr) {
    paste0('http://www.cdc.gov/brfss/annual_data/', yr, '/files/', get_zip_name(yr))
}

get_zip_name <- function(yr) {
    paste0('LLCP',yr,'ASC.ZIP')
}

get_xpt_file_name <- function(yr) {
    paste0('LLCP', yr, '.XPT')
}

get_asc_file_name <- function(yr) {
    paste0('LLCP', yr, '.ASC')
}

get_csv_file_name <- function(yr) {
    paste0('LLCP', yr, '.CSV')
}

## Download the zip files for all years, but only download if necessary
lapply(X=years,FUN=function(yr) { 
    if (!file.exists(get_zip_name(yr))) {
        download.file(url=get_url(yr), 
                      destfile=get_zip_name(yr), 
                      method='wget')
    }})

## unzip all zip files 
lapply(X=years, FUN=function(yr) {  unzip(zipfile=get_zip_name(yr))})


## For each year convert from the fixed width ASCII format (encoded as latin1)
## into CSV format (latin1)

## For each year read the file line by line into a data frame, reading only
## the columns of interest. Note that we have to use the fixed width
## format files because the SAS XPRT ones don't include the calculated
## values and don't include sufficient data to perform all the calculations!

## Each year has different column encodings and, it turns out, that features were 
## only collected or calculated in certain years, so correct for that too.

num_records=-1  
yr=2015
con=file(get_asc_file_name(yr), encoding="latin1")
x <- readLines(con, n=num_records)
mydata <- data.frame("DISPCODE"=substr(x,32 , 35),
                     "RFHLTH"=substr(x,1894 , 1894),
                     "HCVU651"=substr(x,1895 , 1895),
                     ##"RFHYPE5"=substr(x,1896 , 1896),
                     ##"CHOLCHK"=substr(x,1897 , 1897),
                     ##"RFCHOL"=substr(x,1898 , 1898),
                     "MICHD"=substr(x,1899 , 1899),
                     "CASTHM1"=substr(x,1901 , 1901),
                     "DRDXAR1"=substr(x,1903 , 1903),
                     "RACEGR3"=substr(x,1969 , 1969),
                     "AGEG"=substr(x,1976 , 1976),
                     "BMI5CAT"=substr(x,1992 , 1992),
                     "SMOKER3"=substr(x,1997 , 1997))
write.csv(mydata, file=get_csv_file_name(yr), row.names=FALSE)
close(con)

## Check that we've got results that make sense.
mydata.2015 <- read.csv(file=get_csv_file_name(yr))
all(
    c(length(mydata.2015$RFHLTH) == (358072 + 82137 + 1247),
      length(filter(mydata.2015, RFHLTH==1)$RFHLTH) == 358072,
      length(filter(mydata.2015, RFHLTH==2)$RFHLTH) == 82137,
      length(filter(mydata.2015, RFHLTH==9)$RFHLTH) == 1247)
)

all(
    c(length(filter(mydata.2015, SMOKER3==1)$SMOKER3) == 43583,
      length(filter(mydata.2015, SMOKER3==2)$SMOKER3) == 17998,
      length(filter(mydata.2015, SMOKER3==3)$SMOKER3) == 122277,
      length(filter(mydata.2015, SMOKER3==4)$SMOKER3) == 239608,
      length(filter(mydata.2015, SMOKER3==9)$SMOKER3) == 17990)
)

## For years 2011 through 2014 we need to calculate MICHD (because it wasnt
## calculated) - for this we need to collect CVDINFR4 & CVDCRHD4

## michd: 1: CVDINFR4=1 OR CVDCRHD4=1
## 2: CVDINFR4=2 AND CVDCRHD4=2
## blank: CVDINFR4=7, 9 OR MISSING OR CVDCRHD4=7, 9, OR MISSING
michd <- function(I, J) {
    if (I == 1 || J == 1) {
        1
    } else if (I == 2 && J ==2 ) {
        2
    } else {
        " "
    }
}

## We want to use the dplyr package to perform the mutation
require(dplyr)
yr = 2014
con=file(get_asc_file_name(yr), encoding="latin1")
x <- readLines(con, n=num_records)
mydata <- data.frame("DISPCODE"=substr(x,31 , 34),
                     "RFHLTH"=substr(x,2155 , 2155),
                     "HCVU651"=substr(x,2156 ,  2156),
                     ##"RFHYPE5"=substr(x,1896 , 1896),
                     ##"CHOLCHK"=substr(x,1897 , 1897),
                     ##"RFCHOL"=substr(x,1898 , 1898),
                     ##"MICHD"=substr(x,1899 , 1899),
                     "CASTHM1"=substr(x,2159 , 2159),
                     "DRDXAR1"=substr(x,2161 , 2161),
                     "RACEGR3"=substr(x,2230 , 2230),
                     "AGEG"=substr(x,2235 , 2235),
                     "BMI5CAT"=substr(x,2251 , 2251),
                     "SMOKER3"=substr(x,2256 , 2256),
                     "CVDINFR4" = substr(x, 94, 94),
                     "CVDCRHD4" = substr(x, 95, 95)
                     )
mydata <- mutate(mydata, MICHD=michd(CVDINFR4, CVDCRHD4))
write.csv(mydata, file=get_csv_file_name(yr), row.names=FALSE)
close(con)

## Check that we've got results that make sense.
mydata.2014 <- read.csv(file=get_csv_file_name(yr))
all(c(length(mydata.2014$RFHLTH) == (376261 + 86711 + 1692),
      length(filter(mydata.2014, RFHLTH==1)$RFHLTH) == 376261,
      length(filter(mydata.2014, RFHLTH==2)$RFHLTH) == 86711,
      length(filter(mydata.2014, RFHLTH==9)$RFHLTH) == 1692,

      length(filter(mydata.2014, SMOKER3==1)$SMOKER3) == 47122,
      length(filter(mydata.2014, SMOKER3==2)$SMOKER3) == 19242,
      length(filter(mydata.2014, SMOKER3==3)$SMOKER3) == 128629,
      length(filter(mydata.2014, SMOKER3==4)$SMOKER3) == 248500,
      length(filter(mydata.2014, SMOKER3==9)$SMOKER3) == 21171))


yr = 2013
con=file(get_asc_file_name(yr), encoding="latin1")
x <- readLines(con, n=num_records)
mydata <- data.frame("DISPCODE"=substr(x,31 , 34),
                     "RFHLTH"=substr(x,2101 , 2101),
                     "HCVU651"=substr(x,2102 ,  2102),
                     ##"RFHYPE5"=substr(x,1896 , 1896),
                     ##"CHOLCHK"=substr(x,1897 , 1897),
                     ##"RFCHOL"=substr(x,1898 , 1898),
                     ##"MICHD"=substr(x,1899 , 1899),
                     "CASTHM1"=substr(x,2107 , 2107),
                     "DRDXAR1"=substr(x,2109 , 2109),
                     "RACEGR3"=substr(x,2175 , 2175),
                     "AGEG"=substr(x,2180 , 2180),
                     "BMI5CAT"=substr(x,1992 , 1992),
                     "SMOKER3"=substr(x,2201 , 2201),
                     "CVDINFR4" = substr(x, 98, 98),
                     "CVDCRHD4" = substr(x, 99, 99)
                     )
mydata <- mutate(mydata, MICHD=michd(CVDINFR4, CVDCRHD4))
write.csv(mydata, file=get_csv_file_name(yr), row.names=FALSE)
close(con)
## Check that we've got results that make sense.
mydata.2013 <- read.csv(file=get_csv_file_name(yr))
length(mydata.2013$RFHLTH) == (395184 + 94609 + 1980)
length(filter(mydata.2013, RFHLTH==1)$RFHLTH) == 395184
length(filter(mydata.2013, RFHLTH==2)$RFHLTH) == 94609
length(filter(mydata.2013, RFHLTH==9)$RFHLTH) == 1980

length(filter(mydata.2013, SMOKER3==1)$SMOKER3) == 55157
length(filter(mydata.2013, SMOKER3==2)$SMOKER3) == 21455
length(filter(mydata.2013, SMOKER3==3)$SMOKER3) == 138218
length(filter(mydata.2013, SMOKER3==4)$SMOKER3) == 261621
length(filter(mydata.2013, SMOKER3==9)$SMOKER3) == 15322 



yr = 2012
con=file(get_asc_file_name(yr), encoding="latin1")
x <- readLines(con, n=num_records)
mydata <- data.frame("DISPCODE"=substr(x,30 , 33),
                     "RFHLTH"=substr(x,1597 , 1597),
                     "HCVU651"=substr(x,1598 ,  1598),
                     ##"RFHYPE5"=substr(x,1896 , 1896),
                     ##"CHOLCHK"=substr(x,1897 , 1897),
                     ##"RFCHOL"=substr(x,1898 , 1898),
                     ##"MICHD"=substr(x,1899 , 1899),
                     "CASTHM1"=substr(x,1601 , 1601),
                     "DRDXAR1"=substr(x,1603 , 1603),
                     "RACEGR3"=substr(x,1625 , 1625),
                     "AGEG"=substr(x,1632 , 1632),
                     "BMI5CAT"=substr(x,1992 , 1992),
                     "SMOKER3"=substr(x,1653 , 1653),
                     "CVDINFR4" = substr(x, 85, 85),
                     "CVDCRHD4" = substr(x, 86, 86)
                     )
mydata <- mutate(mydata, MICHD=michd(CVDINFR4, CVDCRHD4))
write.csv(mydata, file=get_csv_file_name(yr), row.names=FALSE)
close(con)

## Check that we've got results that make sense.
mydata.2012 <- read.csv(file=get_csv_file_name(yr))
all(c(length(mydata.2012$RFHLTH) == (381050 + 93074 + 1563),
      length(filter(mydata.2012, RFHLTH==1)$RFHLTH) == 381050,
      length(filter(mydata.2012, RFHLTH==2)$RFHLTH) == 93074,
      length(filter(mydata.2012, RFHLTH==9)$RFHLTH) == 1563,

      length(filter(mydata.2012, SMOKER3==1)$SMOKER3) == 54940,
      length(filter(mydata.2012, SMOKER3==2)$SMOKER3) == 21160,
      length(filter(mydata.2012, SMOKER3==3)$SMOKER3) == 135426,
      length(filter(mydata.2012, SMOKER3==4)$SMOKER3) == 254492,
      length(filter(mydata.2012, SMOKER3==9)$SMOKER3) == 9669))


yr=2011
con=file(get_asc_file_name(yr), encoding="latin1")
x <- readLines(con, n=num_records)
mydata <- data.frame("DISPCODE"=substr(x,30 , 33),
                     "RFHLTH"=substr(x,1485 , 1485),
                     "HCVU651"=substr(x,1486 , 1486),
                     ##"RFHYPE5"=substr(x,1487 , 1487),
                     ##"CHOLCHK"=substr(x,1488 , 1488),
                     ##"RFCHOL"=substr(x,1489 , 1489),
                     "CASTHM1"=substr(x,1491 , 1491),
                     "DRDXAR1"=substr(x,1493 , 1493),
                     "RACEGR3"=substr(x,1514 , 1514),
                     "AGEG"=substr(x,1521 , 1521),
                     "BMI5CAT"=substr(x,1537 , 1537),
                     "SMOKER3"=substr(x,1494 , 1494),
                     "CVDINFR4" = substr(x, 89, 89),
                     "CVDCRHD4" = substr(x, 90, 90))
mydata <- mutate(mydata, MICHD=michd(CVDINFR4, CVDCRHD4))
mydata$MICHD <- as.factor(mydata$MICHD)
write.csv(mydata, file=get_csv_file_name(yr), row.names=FALSE)
close(con)

mydata.2011 <- read.csv(file=get_csv_file_name(yr))

## Test that we've read the file back correctly and we have the correct
## number of records
all(c(length(mydata.2011$RFHLTH) == (405485 + 98970 + 2012),
      length(filter(mydata.2011, RFHLTH==1)$RFHLTH) == 405485,
      length(filter(mydata.2011, RFHLTH==2)$RFHLTH) == 98970,
      length(filter(mydata.2011, RFHLTH==9)$RFHLTH) == 2012,

      length(filter(mydata.2011, SMOKER3==1)$SMOKER3) == 62027,
      length(filter(mydata.2011, SMOKER3==2)$SMOKER3) == 22724,
      length(filter(mydata.2011, SMOKER3==3)$SMOKER3) == 147864,
      length(filter(mydata.2011, SMOKER3==4)$SMOKER3) == 271310,
      length(filter(mydata.2011, SMOKER3==9)$SMOKER3) == 2542))

## Ensure that the directory exists in hdfs

system2('hdfs', args=c('dfs', '-mkdir', '-p', 'brfss'))

## Move the csv files into hdfs
lapply(X=years, FUN=function(yr) { system2('hdfs',args=c('dfs','-put', '-f', get_csv_file_name(yr), 'brfss'))})


