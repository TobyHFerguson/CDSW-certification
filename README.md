# CDSW-certification
Repo to be used to certify for the SME CDSW team

# Overview
We're interested in seeing whether self-reported health measures of a person, along with some 
of their demographic information, might enable us to predict their asthmatic status. 

The data we have to work with is five years of telephone surveys taken from across the US
where respondents are asked to report on a variety of health conditions and demographics, including
their asthmatic status.



# Data Set
The data sets for health come from the [CDC's Behavior Risk Factor Surveillance System 
(aka BRFSS)](http://www.cdc.gov/brfss). These data sets cover many years but we'll limit 
ours to the years 2011-2015 because this set can be compared one against the other.

These data sets contain much information. For convenience we will largely
use the calculated data rather than the raw data. 

The information (and the corresponding column header) 
we're interested in includes:

## Raw Data
Variable Name | Meaning 
--------------|---------
DISPCODE | Was the interview completed or not 
\_RFHLTH | Adults with good or better health 
\_HCVU651 | Adults with Health Care coverage 
\_MICHD | Respondents with heart issues
\_CASTHM1 | Adult currently has asthma
\_DRDXAR1 | Diagnosed with Arthritis
\_RACEGR3 | Five-level race group
\_AGE_G | Six level age group 
\_BMI5CAT | BMI Category
\_SMOKER3 | Four Level Smoke Status
CVDINFR4 | heart attack
CVDCRHD4 | Angina

I had hoped to investigate the relationship between cholesterol and asthma but the data
sets for the years 2011 through 2014 dont contain any cholesterol information.

The data is available in SAS XPORT and fixed width formats. The SAS format doesn't include sufficient
data to calculate race properly, so we'll use the fixed width format.

For statistically accurate reporting sophisticated weighting models must be used in the analysis of this data. 
For our purposes (certification for SME CDSW team) we're more interested in the processing, 
so will not apply those models but simply use the raw data as is.

The data along with codebooks can be found here:

[https://www.cdc.gov/brfss/annual_data/annual_data.htm](https://www.cdc.gov/brfss/annual_data/annual_data.htm)

# Data Ingest
The data is in the form of zip files, each file of the order of a few MB. 

The ingest process does the minimum necessary to get the data into HDFS:
* download each zip file locally into a data directory (`$HOME/brfss`)
* unzip each file into a fixed width file for that year (`$HOME/brfss/YEAR.txt`)
* write that file into HDFS (`$HDFS_HOME/brfss/YEAR.txt`)

# Data Processing
The files are in fixed width format, and location of the fields differ in each of the five years. Furthermore
the fields present are different for different years. We have to calculate fields for the years where they're
missing.

## Field extraction
We use a json codebook file, indexed by year, to contain this fixed width content. We then use an observer 
function (`xtract`) which will extract a named field's contents given the spark data frame and
the json codebook contents for the relevant year.

## Field Calculation
### 2015
The 2015 year has all fields and needs no further processing.
### Other years
\_RACEGR3 is known as \_RACEGR2 in 2011 & 2012. If we find a year in which \_RACEGR3 is not present then we
create a new \_RACEGR3 with the value of \_RACEGR2

\_MICHD is not present in the years 2011 thru 2014 so is calculated as defined in the [2015 Codebook](https://www.cdc.gov/brfss/annual_data/2015/pdf/codebook15_llcp.pdf) 

# Target Response
\_CASTHMS1 is the target field indicating whether someone is asthmatic (\_CASTHMS1 has the value 1) or not 
(\_CASTHMS1 has some other value)
