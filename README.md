# CDSW-certification
Repo to be used to certify for the SME CDSW team

# Overview
We'll attempt to correlate data about people who say they've got asthma with data about air quality.

The kinds of questions we're interested in include:
+ How has Asthma varied by year over the entire US
+ How has Asthma varied by year over regions within the US
+ How has air quality varied by year over the entire US
+ How has air quality varied by year over regions with the US
+ What is the correlation between asthma and air quality?

# Data Sets
## Health
The data sets for health come from the CDC's Behavior Risk Factor Surveillance System (aka BRFSS). These data sets cover many years but we'll limit ours to the years 2011-2015 because this set can be compared one against the other.

These data sets contain much information. The information (and the corresponding column header) we're interested in includes:
+ _STATE: Which state is the respondent in?
+ ASTHMA3 Has the respondent ever had asthma?
+ ASTHNOW Does the respondent have asthma now?

We will consider anyone who self reports as having asthma or who has been told they have asthma as having asthma.

The data is available in SAS XPORT formats. We will use the `foreign::read.xport` function in R to read these files and convert them to simple csv files containing 

For statistically accurate reporting sophisticated weighting models must be used in the analysis of this data. For our purposes (certification for SME CDSW team) we're more interested in the processing, so will not apply those models but simply use the raw data as is.

Data urls with codebooks are:
Year | Data 
-----|-----------------
2011 | http://www.cdc.gov/brfss/annual_data/2011/LLCP2011XPT.zip 
2012 | http://www.cdc.gov/brfss/annual_data/2012/files/LLCP2012XPT.ZIP 
2013 | http://www.cdc.gov/brfss/annual_data/2013/files/LLCP2013XPT.ZIP 
2014 | http://www.cdc.gov/brfss/annual_data/2014/files/LLCP2014XPT.ZIP 
2015 | https://www.cdc.gov/brfss/annual_data/2015/files/LLCP2015XPT.zip

The codebooks for these years are identical for the features we're interested in. The [2015 codebook](https://www.cdc.gov/brfss/annual_data/2015/pdf/codebook15_llcp.pdf) will do.

## Air quality
Air quality data comes from the Environmental Protection Agency (EPA) using their [Pre-Generated Data Files](http://aqsdr1.epa.gov/aqsweb/aqstmp/airdata/download_files.html)

We only consider the annual data files and their [codebook](http://aqsdr1.epa.gov/aqsweb/aqstmp/airdata/FileFormats.html#_annual_summary_files)

The features of interest are:
+ State Code
+ Parameter Code (of which the value 88502 means "PM2.5", the air quality measurement of interest)
+ Year
+ Completeness Indicator

We will only consider a measurement when it occurs in the year we're interested in, and is complete.
Data urls are:
Year | Data
-----|------
2011 | http://aqsdr1.epa.gov/aqsweb/aqstmp/airdata/annual_all_2011.zip
2012 | http://aqsdr1.epa.gov/aqsweb/aqstmp/airdata/annual_all_2012.zip
2013 | http://aqsdr1.epa.gov/aqsweb/aqstmp/airdata/annual_all_2013.zip
2014 | http://aqsdr1.epa.gov/aqsweb/aqstmp/airdata/annual_all_2014.zip
2015 | http://aqsdr1.epa.gov/aqsweb/aqstmp/airdata/annual_all_2015.zip

# Data Sources
