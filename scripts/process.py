import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.functions import col
from pyspark.sql.types import *

# Define some functions to abstract the file locations

def get_rawfile(year): return str(year)+".txt"
def get_rawpath(year): return "brfss/" + get_rawfile(year)

def get_parquetfile(year): return str(year)+".parquet"
def get_parquetpath(year): return "brfss/" + get_parquetfile(year)

def get_codebook(year): return "scripts/" +str(year)+"_code.json"

 
# Start the Spark session     
spark = SparkSession \
  .builder\
  .appName("BRFSS")\
  .getOrCreate()
  


# Helper function to extract the relevant part of the fixed length record  
def xtract(frame, code, alias): 
  start = code[alias]["COL"]
  length = code[alias]["LENGTH"]
  colname =  alias.lstrip("_")
  return frame.value.substr(start,length).alias(colname).cast(IntegerType()) 

# Define a helper function to correct the DISPCODE column
def dispcode(c):
  return c if c > 1000 else c * 10
  
dispcodeu = udf (dispcode, IntegerType())

# 2015 has the _MICHD column already defined
# Extract the relevant columns and write the dataframe to a parquet file
for year in xrange(2015,2017):
  pfile=get_parquetpath(year)
  try:
    spark.read.text(pfile)
    print(str(year) + " parquet file already exists. Skipping")
    continue
  except:
    pass
  
  print("creating parquet file " + pfile +" for: "+str(year))
  with open(get_codebook(year)) as yj:
    data = json.load(yj)
    df = spark.read.text(get_rawpath(year))
    newdf = df.select(
                xtract(df, data, "CVDCRHD4"),
                xtract(df, data, "CVDINFR4"),
                xtract(df, data, "DISPCODE"),
                xtract(df, data, "RENTHOM1"),
                xtract(df, data, "SEQNO"),
                xtract(df, data, "SEX"),
                xtract(df, data, "_AGE_G"),
                xtract(df, data, "_BMI5CAT"),
                xtract(df, data, "_CASTHM1"),
                xtract(df, data, "_DRDXAR1"),
                xtract(df, data, "_EDUCAG"),
                xtract(df, data, "_HCVU651"),
                xtract(df, data, "_INCOMG"),
                xtract(df, data, "_MICHD"),
                xtract(df, data, "_RACEGR3"),
                xtract(df, data, "_RFHLTH"),
                xtract(df, data, "_SMOKER3"),
                xtract(df, data, "_STATE")
              )
    newdf.withColumn("DISPCODE", dispcodeu(newdf.DISPCODE)).write.parquet(pfile, mode="overwrite")

#Some test stuff
#df = spark.read.parquet(get_parquetpath(year))
#df.select()
#df.count()

# Define some helper functions so we can create the _MICHD column
def michd(I, J): 
    if (I == 1 or J == 1): return 1
    elif (I == 2 and J ==2 ): return 2

michdu = udf (michd, IntegerType())             

# Extract the relevent data frames for 2011 thru' 2014 and write to parquet'    
for year in xrange(2011,2015):
  pfile=get_parquetpath(year)
  try:
    spark.read.text(pfile)
    print(str(year) + " parquet file already exists. Skipping")
    continue
  except:
    pass
  
  print("creating parquet file " + pfile +" for: "+str(year))
  with open(get_codebook(year)) as yj:
    data = json.load(yj)
    try:
      data["_RACEGR3"] = data["_RACEGR2"]
    except KeyError:
      pass
    df = spark.read.text(get_rawpath(year))
    newdf = df.select(
                xtract(df, data, "CVDCRHD4"),
                xtract(df, data, "CVDINFR4"),
                xtract(df, data, "DISPCODE"),
                xtract(df, data, "RENTHOM1"),
                xtract(df, data, "SEQNO"),
                xtract(df, data, "SEX"),
                xtract(df, data, "_AGE_G"),
                xtract(df, data, "_BMI5CAT"),
                xtract(df, data, "_CASTHM1"),
                xtract(df, data, "_DRDXAR1"),
                xtract(df, data, "_EDUCAG"),
                xtract(df, data, "_HCVU651"),
                xtract(df, data, "_INCOMG"),
                xtract(df, data, "_RACEGR3"),
                xtract(df, data, "_RFHLTH"),
                xtract(df, data, "_SMOKER3"),
                xtract(df, data, "_STATE")
              )
    newdf.withColumn("MICHD", michdu(newdf.CVDINFR4, newdf.CVDCRHD4)).withColumn("DISPCODE", dispcodeu(newdf.DISPCODE))\
        .write.parquet(pfile, mode="overwrite")

records_per_year = {2011: 506467,
                    2012: 475687,
                    2013: 491773,
                    2014: 464664,
                    2015: 441456,
                    2016: 486303
                   }
for year in xrange(2011,2017):
  df = spark.read.parquet(get_parquetpath(year))
  nr=df.count()
  if (nr == records_per_year[year]):
    print(str(year)+" has " +str(nr)+" records")
  else:
    print(str(year)+" has " + str(nr) + " records but it should've been " +str(records_per_year[year]))