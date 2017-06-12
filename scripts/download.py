import json
import os
from pyspark.sql import SparkSession
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
  
os.chdir(data_dir)

zips = { '2015': "https://www.cdc.gov/brfss/annual_data/2015/files/LLCP2015ASC.zip",
        '2014': "http://www.cdc.gov/brfss/annual_data/2014/files/LLCP2014ASC.ZIP",
        '2013': "http://www.cdc.gov/brfss/annual_data/2013/files/LLCP2013ASC.ZIP",
        '2012': "http://www.cdc.gov/brfss/annual_data/2012/files/LLCP2012ASC.ZIP",
        '2011': "http://www.cdc.gov/brfss/annual_data/2011/files/LLCP2011ASC.ZIP"
        }

for y in zips:
  url = zips[y]
  zfile = url.split('/')[-1]
  urllib.urlretrieve(url,zfile)
  with zipfile.ZipFile(zfile, 'r') as zf:
    for z in zf.filelist:
      outfile=y+'.txt'
      zf.extract(z, outfile)
      subprocess.check_call(['hdfs','dfs', '-put', '-f', data_dir +"/" + outfile])
      
spark = SparkSession \
  .builder\
  .appName("BRFSS")\
  .getOrCreate()
  

def xtract(frame, code, alias):
  try:
    return frame.value.substr(code[alias]["COL"],code[alias]["LENGTH"]).alias(alias)
  except KeyError:
    return "FIXME"

code_dir = 'scripts'
with open(code_dir + '/' + '2015_code.json') as yj:
  data = json.load(yj)
  df = spark.read.text("brfss/2015.txt")
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
            )
  
  
