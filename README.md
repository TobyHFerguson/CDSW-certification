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

These data sets contain much information. The information (and the corresponding column header) 
we're interested in includes:

## Raw Data
Variable Name | Meaning 
--------------|---------
DISPCODE | Was the interview completed or not (1100: completed; 1200: not completed)
_STATE | Which state is the respondent in? (FIPS code)
ASTHMA3 | Has the respondent ever had asthma? (1: Yes; 9: Refused)
ASTHNOW | Does the respondent have asthma now? (1: Yes; 9: Refused)
SEX | Sex of respondent (1: Male; 2: Female)
SMOKE100 | Smoked at least 100 cigarettes in life (1:Yes; 9: Refused)
SMOKDAY2 | Smoke now (1: Every day; 2: Some days; 9: Refused)
ASTHMAGE | Age at Asthma diagnosis (11-96: Age 11-96+; 97: <=10; 98: Don't know; 99: Refused)
ASATTACK | Asthma or Asthma attack in last 12 months (1: Yes: 2: No; 7: Unsure; 9: Refused)
ASERVIST | Emergency care for asthma in last 12 months (1-87: # of visits; 88: none; 98: Don't know; 99: Refused)
ASDRVIST | Urgent visits for Asthma (1-87: # of visits; 88: None; 98 Don't know; 99: Refused)
ASRCHKUP | Routine visits for Asthma (1-87: # of visits; 88: None; 98 Don't know; 99: Refused)
ASYMPTOM | Asthma symptoms during past 30 days (Complex - refer to [2015 codebook](https://www.cdc.gov/brfss/annual_data/2015/pdf/codebook15_llcp.pdf) will do.
)
ASNOSLEP | How many days did asthma make it hard to sleep in the last 30 days?
ASTHMED3 | In last 30 days how many days did you take medication to prevent an asthma attack
ASINHALR | In last 30 days how many days did you use an inhaled during an asthma attack
CASTHDX2 | Has child ever been diagnosed with Asthma?
CASTHNO2 | Has child still got asthma?

However we might do better to start with the calculated values that provide a more compressed
version of the data. 

The idea here would be to see how asthma might correlate to other aspects of the person:
## Calculated Data
_RFHLTH | Adults with good or better health (1: Good; 2: Fair; 3: Other)
_HCVU651 | Adults with Health Care coverage (1: Yes; 2: No; 3: Other)
_RFHYPE5 | Adults with High blood pressure (1: No; 2: Yes; 3: Other)
_MICHD | Respondents with heart issues (1: Yes; 2: No; 3: Other)
_LTASH1 | Adult has been told they have asthma in lifetime (1: No; 2: Yes; 9: other)
_CASTHM1 | Adult currently has asthma (1: No; 2: Yes; 9: Other)
_ASTHMS1 | Computed Asthma Status (1: Current; 2: Former; 3: Never; 9: Other)
_DRDXAR1 | Diagnosed with Arthritis (1: Yes; 2: No; 3: Other)
_RACEGR3 | Five-level race group
_AGE_G | Six level age group
_SMOKER3 | Four Level Smoke Status


The data is available in SAS XPORT formats. We will use the `foreign::read.xport` 
function in R to read these files and convert them to simple csv files containing 

For statistically accurate reporting sophisticated weighting models must be used in the analysis of this data. 
For our purposes (certification for SME CDSW team) we're more interested in the processing, 
so will not apply those models but simply use the raw data as is.

Data urls with codebooks are:

Year | Data 
-----|-----------------
2011 | http://www.cdc.gov/brfss/annual_data/2011/LLCP2011XPT.zip 
2012 | http://www.cdc.gov/brfss/annual_data/2012/files/LLCP2012XPT.ZIP 
2013 | http://www.cdc.gov/brfss/annual_data/2013/files/LLCP2013XPT.ZIP 
2014 | http://www.cdc.gov/brfss/annual_data/2014/files/LLCP2014XPT.ZIP 
2015 | https://www.cdc.gov/brfss/annual_data/2015/files/LLCP2015XPT.zip

The codebooks for these years are identical for the features we're interested in. 
The [2015 codebook](https://www.cdc.gov/brfss/annual_data/2015/pdf/codebook15_llcp.pdf) will do.

