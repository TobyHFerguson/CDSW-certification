import json
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.functions import col
from pyspark.sql.types import *
import subprocess
import urllib
import zipfile

# Create a temporary data directory both locally and on hdfs.
data_dir = "brfss"
try:
  os.mkdir(data_dir)
except OSError:
  pass
else:
  subprocess.check_call(['hdfs', 'dfs', '-mkdir', '-p', data_dir])
  
zips = { '2015': "https://www.cdc.gov/brfss/annual_data/2015/files/LLCP2015ASC.zip",
        '2014': "http://www.cdc.gov/brfss/annual_data/2014/files/LLCP2014ASC.ZIP",
        '2013': "http://www.cdc.gov/brfss/annual_data/2013/files/LLCP2013ASC.ZIP",
        '2012': "http://www.cdc.gov/brfss/annual_data/2012/files/LLCP2012ASC.ZIP",
        '2011': "http://www.cdc.gov/brfss/annual_data/2011/files/LLCP2011ASC.ZIP"
        }

# Define some functions to abstract the file locations

def get_rawfile(year): return str(year)+".txt"
def get_rawpath(year): return "brfss/" + get_rawfile(year)

def get_parquetfile(year): return str(year)+".parquet"
def get_parquetpath(year): return "brfss/" + get_parquetfile(year)

def get_codebook(year): return "scripts/" +str(year)+"_code.json"

# Download the zips and get the contents into files in hdfs
for y in zips:
  url = zips[y]
  zfile = data_dir +"/"+url.split('/')[-1]
  urllib.urlretrieve(url,zfile)
  with zipfile.ZipFile(zfile, 'r') as zf:
    for z in zf.namelist():
      zf.extract(z)
      outfile=get_rawpath(y)
      os.rename(z, outfile)           
      subprocess.check_call(['hdfs','dfs', '-put', '-f', outfile, outfile])
 
# Start the Spark session     
spark = SparkSession \
  .builder\
  .appName("BRFSS")\
  .getOrCreate()
  


# Helper function to extract the relevant part of the fixed length record  
def xtract(frame, code, alias): return frame.value.substr(code[alias]["COL"],code[alias]["LENGTH"]).alias(alias.lstrip("_")).cast(IntegerType()) 

# 2015 has the _MICHD column already defined
# Extract the relevant columns and write the dataframe to a parquet file
year = 2015
with open(get_codebook(year)) as yj:
  data = json.load(yj)
  df = spark.read.text(get_rawpath(year))
  df.select(
            xtract(df, data, "DISPCODE"),
            xtract(df, data, "_RFHLTH"),
            xtract(df, data, "_HCVU651"),
            xtract(df, data, "_MICHD"),
            xtract(df, data, "_CASTHM1"),
            xtract(df, data, "_DRDXAR1"),
            xtract(df, data, "_RACEGR3"),
            xtract(df, data, "_AGE_G"),
            xtract(df, data, "_BMI5CAT"),
            xtract(df, data, "_SMOKER3"),
            xtract(df, data, "CVDINFR4"),
            xtract(df, data, "CVDCRHD4")
            ).write.parquet(get_parquetpath(year), mode="overwrite")


# Define some helper functions so we can create the _MICHD column
def michd(I, J): 
    if (I == 1 or J == 1): return 1
    elif (I == 2 and J ==2 ): return 2

michdu = udf (michd, IntegerType())             

# Extract the relevent data frames for 2011 thru' 214 nd write to parquet'    
for year in xrange(2011,2014):
  with open(get_codebook(year)) as yj:
    data = json.load(yj)
    try:
      data["_RACEGR3"] = data["_RACEGR2"]
    except KeyError:
      pass
    df = spark.read.text(get_rawpath(year))
    newdf = df.select(
              xtract(df, data, "DISPCODE"),
              xtract(df, data, "_RFHLTH"),
              xtract(df, data, "_HCVU651"),
              xtract(df, data, "_CASTHM1"),
              xtract(df, data, "_DRDXAR1"),
              xtract(df, data, "_RACEGR3"),
              xtract(df, data, "_AGE_G"),
              xtract(df, data, "_BMI5CAT"),
              xtract(df, data, "_SMOKER3"),
              xtract(df, data, "CVDINFR4"),
              xtract(df, data, "CVDCRHD4")
              )
    newdf.withColumn("MICHD", michdu(newdf.CVDINFR4, newdf.CVDCRHD4))\
        .write.parquet(get_parquetpath(year), mode="overwrite")

for y in zips:
  
