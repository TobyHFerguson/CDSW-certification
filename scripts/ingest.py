
# Python 2
import os
import subprocess
import urllib
import zipfile

# Create a temporary data directory both locally and on hdfs.
data_dir = "brfss"
try:
  os.mkdir(data_dir)
except OSError:
  pass

subprocess.check_call(['hdfs', 'dfs', '-mkdir', '-p', data_dir])
  
zips = { '2015': "https://www.cdc.gov/brfss/annual_data/2015/files/LLCP2015ASC.zip",
        '2014': "http://www.cdc.gov/brfss/annual_data/2014/files/LLCP2014ASC.ZIP",
        '2013': "http://www.cdc.gov/brfss/annual_data/2013/files/LLCP2013ASC.ZIP",
        '2012': "http://www.cdc.gov/brfss/annual_data/2012/files/LLCP2012ASC.ZIP",
        '2011': "http://www.cdc.gov/brfss/annual_data/2011/files/LLCP2011ASC.ZIP"
        }

zips = {'2016': "https://www.cdc.gov/brfss/annual_data/2016/files/LLCP2016ASC.zip"}
# Define some functions to abstract the file locations

def get_rawfile(year): return str(year)+".txt"
def get_rawpath(year): return "brfss/" + get_rawfile(year)

def get_parquetfile(year): return str(year)+".parquet"
def get_parquetpath(year): return "brfss/" + get_parquetfile(year)

def get_codebook(year): return "scripts/" +str(year)+"_code.json"

# Download the archives and get the contents into files in hdfs
# Commented out since the files are now in hdfs
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
      os.remove(outfile)
      os.remove(zfile)
      
os.removedirs(data_dir)
 