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
_RFHLTH | Adults with good or better health 
_HCVU651 | Adults with Health Care coverage 
_MICHD | Respondents with heart issues
_CASTHM1 | Adult currently has asthma
_DRDXAR1 | Diagnosed with Arthritis
_RACEGR3 | Five-level race group
_AGE_G | Six level age group 
_BMI5CAT | BMI Category
_SMOKER3 | Four Level Smoke Status
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

We will:
* download each zip file locally into a data directory (`$HOME/brfss`)
* unzip each file into a fixed width file for that year
* convert that file into a CSV file, calculating MICHD as needed
* copy that CSV file into an hdfs directory (`/user/cdsw/brfss/`)

We will need to provide some kind of count or frequency distribution of anomalous records (records
where the data is either missing or incomplete) and determine how to deal with them. 
(Probably eliminate them).


# Data Prep
We will try to figure out whether someone is asthmatic (i.e. _ASTHMS1 has the value 1)