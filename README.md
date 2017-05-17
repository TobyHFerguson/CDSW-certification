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
Variable Name | Meaning | Type | Coding 
--------------|------|---------|--------
DISPCODE | Was the interview completed or not | Categorical | 1100: completed; 1200: not completed
_STATE | Which state is the respondent in? | Categorical | FIPS code
GENHLTH | General health | Categorical | 1: Excellent .. 5: Poor, 7: Don't know; 9: Refused
PHYSHLTH | Num days bad physhealth | Continuous | 1 - 30; 88: None; 77: Don't know; 99: refused
MENTHLTH | Num days bad mental health | Continuous | 1 - 30; 88: None; 77: Don't know; 99: refused
POORHLTH | Num days activity restricted | Continuous | 1 - 30; 88: None; 77: Don't know; 99: refused 
HLTHPLN1 | Access to health plan | Categorical | 1: yes; 2: no; 7: Not sure; 9: refused
PERSDOC2 | Personal doctor | Categorical | 1: Yes, just one; 2: Yes, more than one; 3: no; 7: Don't know; 9: Refused
MEDCOST | No doctor because of cost | Categorical | 1: Yes; 2: No; 7: Not sure; 9: Refused 
CHECKUP1 | How long since routine checkup | Categorical | 1: < 12mo; 2: 1-2yr; 3: 2-5yr; 4: >5yr; 7; Don't know; 8: never; 9 refused
BPHIGH4 | Hypertension | Categorical | 1: yes; 2: yes, pregnant; 3: no; 4: borderline; 7: Don't know; 9: refused
BPMEDS | Meds for high blood pressure | Categorical | 1: yes; 2: no; 7: Don't know; 9: refused
BLOODCHO | Cholesterol checked | Categorical | 1: Yes; 2: No; 7: Don't know; 9: Refused
CHOLCHK | How long since cholesterol checked | Categorical | 1: <1yr; 2: 1-2yrs; 3: 2-5yrs; 4: >5yrs; 7: Don't know; 9: refused
TOLDHI2 | Told cholesteral is high | Categorical | 1: yes; 2: no; 7: Not sure; 9: refused
CVDINFR4 | Heart attack? | Categorical | 1: yes; 2: no; 7: Not sure; 9: refused
CVDCRHD4 | Angina | Categorical | 1: yes; 2: no; 7: Not sure; 9: refused
CVDSTRK3 | Stroke? | Categorical | 1: yes; 2: no; 7: Not sure; 9: refused
ASTHMA3 | Has the respondent ever had asthma? | Categorical | 1: yes; 2: no; 7: Not sure; 9: refused
ASTHNOW | Does the respondent have asthma now? | Categorical | 1: yes; 2: no; 7: Not sure; 9: refused
CHCSCNCR | Skin Cancer | Categorical | 1: yes; 2: no; 7: Not sure; 9: refused
CHCOCNCR | Other cancer | Categorical | 1: yes; 2: no; 7: Not sure; 9: refused
CHCCOPD1 | Emphysema | Categorical | 1: yes; 2: no; 7: Not sure; 9: refused
HAVARTH3 | Arthritis?  | Categorical | 1: yes; 2: no; 7: Not sure; 9: refused
ADDEPEV2 | Depressive | Categorical | 1: yes; 2: no; 7: Not sure; 9: refused
CHCKIDNY | Kidney disease? | Categorical | 1: yes; 2: no; 7: Not sure; 9: refused
DIABETE3 | Diabetes | Categorical | 1: yes; 2: yes, pregnant; 3: no; 4: borderline; 7: Don't know; 9: refused
DIABAGE2 | Diabetes age | Continuous | 1-97: age; 98: Don't know; 99: refused
SEX | Sex of respondent| Categorical | 1: Male; 2: Female
MARITAL | Marital status | 1: Married; 2: Divorced; 3: Widowed; 4: Separated; 5: never; 6: unmarried couple; 9: refused
EDUCA | Education achieved | Categorical | 1: Kindergarten; 2: Elementary; 3: High school; 4: Hight school graduate; 5: college 1-3 years; 6: College graduate; 9: refused
INCOME2 | Annual income | Categorical | 
WEIGHT2 | Weight | Continuous
HEIGHT3 | Height | Continuous
PREGNANT | Pregnant? | Categorical

SMOKE100 | Smoked at least 100 cigarettes in life (1:Yes; 9: Refused)
SMOKDAY2 | Smoke now (1: Every day; 2: Some days; 9: Refused)
ALCDAY5 | Days with drink | 
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

### Data processing notes
How will we cope with records that contain 'dont know' or 'refused'? 

Some features depend on others and are encoded as blanks when the premise is false. When the premise is false
we will fill in the dependent with a no if it was recorded as blank.

PPRHLTH blank when PHYSHLTH or MENTHLTH is 88. Assign 88 (None).
BPMEDS blank when BPHIGH4 is not yes. Assign NO to blanks when BPHIGH4 is 1,2, 3 or 4; 
CHOLCHK blank when BLOODCHO is 2,7,9 or blank. 
TOLDHI blank when BLOOKDCHO is 2,7,9 or blank.
DIABAGE2 blank when DIABETE3 is 2,3,4,7, 9 or blank
WEIGHT2 need to convert to pounds

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

