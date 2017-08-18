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

## Data of Interest
The field names and the descriptions we'll use are:

Variable Name | Description
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

## Field Value Meaning & Count by Year
The meaning of field values and the count of those values by year is shown below. We're only going to use
the field value count as a sanity check so only two random fields (\_DISPCODE & \_CASTHM1) have been included in that part of the table.

Some of the fields in the original data used a BLANK value to indicate uncertain data. We will convert all BLANK values
to a 9 during the data processing phase.

### \_DISPCODE
Note that in 2011 the code was 110 and 120, as compared to 1100, 1200 for later years. This will be corrected during 
the data processing phase.

Value | Meaning | 2011 | 2012 | 2013 | 2014 | 2015
---|---|---|---|---|---|---
1100 | Completed interview | 463,716 | 441,608 | 433,220 | 413,558 | 375,059
1200 | Partially completed interview | 42,751 | 34,079 | 58,553 | 51,106 | 66,397

### \_RFHLTH
Value | Meaning 
---|---
1 | Good or Better Health
2 | Fair or Poor Health
9 | Unsure or missing

### \_HCVU651
Value | Meaning 
---|---
1 | Have health care coverage
2 | Do not have health care coverage
9 | Don’t know/Not Sure, Refused or Missing

### \_MICHD
Value | Meaning 
---|---
1 | Reported having heart problem
2 | Did not report having heart problem

### \_CASTHM1
Value | Meaning | 2011 | 2012 | 2013 | 2014 | 2015
---|---|---|---|---|---|---
1 | No | 457,964 |429,280 | 442,718 | 418,561 | 398,154
2 | Yes | 45,203 | 43,267 | 45,630 | 42,875 | 40,000
9 | Don’t know/Not Sure Or Refused/Missing | 3,300 | 3,140 | 3,425 |3,228 | 3,302

### \_DRDXAR1
Value | Meaning 
---|---
1 | Diagnosed with arthritis
2 | Not diagnosed with arthritis
BLANK | Don´t know/Not Sure/Refused/Missing

### \_RACEGR3
Value | Meaning 
---|---
1 | White only, Non-Hispanic 
2 |Black only, Non-Hispanic 
3 |Other race only, Non-Hispanic
4 |Multiracial, Non-Hispanic
5 |Hispanic
9 |Don’t know/Not sure/Refused

### \_AGE_G
Value | Meaning 
---|---
1 |Age 18 to 24
2 |Age 25 to 34
3 |Age 35 to 44
4 |Age 45 to 54
5 |Age 55 to 64
6 |Age 65 or older

### \_BMI5CAT
Value | Meaning 
---|---
1 |Underweight
2 |Normal Weight
3 |Overweight
4 |Obese
BLANK |Don’t know/Refused/Missing

### \_SMOKER3
Value | Meaning 
---|---
1 |Current smoker - now smokes every day
2 |Current smoker - now smokes some days
3 |Former smoker
4 |Never smoked
9 |Don’t know/Refused/Missing

### CVDINFR4
Value | Meaning 
---|---
1 |Yes
2 |No  
7 |Don’t know/Not sure  
9 |Refused  
BLANK |Not asked or Missing

### CVDCRHD4
Value | Meaning 
---|---
1 |Yes
2 |No
7 |Don’t know/Not sure 
9 |Refused

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

## Field Calculation & Correction.
### 2015
The 2015 year has all fields and needs no further processing.
### Other years
\_RACEGR3 is known as \_RACEGR2 in 2011 & 2012. If we find a year in which \_RACEGR3 is not present then we
create a new \_RACEGR3 with the value of \_RACEGR2

If we find a value of 110 or 120 in the DISPCODE field we will replace it with 1100 or 1200

If we find a BLANK value in any field we drop the record. This is the default behavior of the
mechanism used to read the records so makes the coding easier. 

\_MICHD is not present in the years 2011 thru 2014 so is calculated as defined in the [2015 Codebook](https://www.cdc.gov/brfss/annual_data/2015/pdf/codebook15_llcp.pdf) 

## Validation
We create Impala tables using the following technique:

First, figure out what the parquet files are called:

```bash
cdsw@psjfrp91wt2nw5rn:~$ hdfs dfs -ls brfss/*.parquet/part-00000-* | awk '{ print $8 }'
brfss/2011.parquet/part-00000-7b4d4723-9872-4d17-b921-bfcd8c1c9c66.snappy.parquet
brfss/2012.parquet/part-00000-699da841-6c73-449b-ad4a-68def324d033.snappy.parquet
brfss/2013.parquet/part-00000-833cbf0c-5b15-4d4d-8efe-ad01ab07ba3e.snappy.parquet
brfss/2014.parquet/part-00000-80fcdcd0-d1bc-4ad2-8d4c-063294d7764a.snappy.parquet
brfss/2015.parquet/part-00000-843db927-4e9c-4a57-be9c-a29f17f3b5a6.snappy.parquet
```

Then, using Hue, create the external tables in the Impala editor:
```sql
create external table brfss_2011 like parquet '/user/clouderanT/brfss/2011.parquet/part-00000-7b4d4723-9872-4d17-b921-bfcd8c1c9c66.snappy.parquet' 
stored as parquet location '/user/clouderanT/brfss/2011.parquet';
create external table brfss_2012 like parquet '/user/clouderanT/brfss/2012.parquet/part-00000-699da841-6c73-449b-ad4a-68def324d033.snappy.parquet' 
stored as parquet location '/user/clouderanT/brfss/2012.parquet';
create external table brfss_2013 like parquet '/user/clouderanT/brfss/2013.parquet/part-00000-833cbf0c-5b15-4d4d-8efe-ad01ab07ba3e.snappy.parquet' 
stored as parquet location '/user/clouderanT/brfss/2013.parquet';
create external table brfss_2014 like parquet '/user/clouderanT/brfss/2014.parquet/part-00000-80fcdcd0-d1bc-4ad2-8d4c-063294d7764a.snappy.parquet'    
stored as parquet location '/user/clouderanT/brfss/2014.parquet';
create external table brfss_2015 like parquet '/user/clouderanT/brfss/2015.parquet/part-00000-843db927-4e9c-4a57-be9c-a29f17f3b5a6.snappy.parquet'    
stored as parquet location '/user/clouderanT/brfss/2015.parquet';
```

We then checked some sample counts to see if the data had been read and converted correctly:

```sql
select count(dispcode) from brfss_2011 where dispcode = 1100;

463716
```
This is a correct value.

```sql
select count(dispcode) from brfss_2014 where dispcode = 1200;

51106
```
This is a correct value.
```sql
select count(casthm1) from brfss_2012 where casthm1 = 9;

3140
```
This is a correct value.
```sql
select count(casthm1) from brfss_2013 where casthm1 = 1;

442713
```
This is an **incorrect** value. It should be 447218 according to the [2013 code book](https://www.cdc.gov/brfss/annual_data/2013/pdf/codebook13_llcp.pdf).
```sql
select count(casthm1) from brfss_2013 where casthm1 = 2;

45630
```
This is a correct value
```sql
select count(casthm1) from brfss_2013 where casthm1 = 9;

3425
```
This is a correct value

```sql
select count(dispcode) from brfss_2013 where dispcode = 1100;

433220
```
This is a correct value
```sql
select count(dispcode) from brfss_2013 where dispcode = 1200;

58553
```

So we have one incorrect value, where a 3 (our data) should be an 8. I think I'm going to ignore it for now!



# Target Response
\_CASTHMS1 is the target field indicating whether someone is asthmatic (\_CASTHMS1 has the value 1) or not 
(\_CASTHMS1 has some other value)
