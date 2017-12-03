! pip install datarobot
%matplotlib inline

from pyspark.sql import SparkSession
import os
from sklearn.cross_validation import train_test_split
import numpy
import datarobot as dr
import pandas as pd
import matplotlib.pyplot as plt

def get_parquetfile(year): return str(year)+".parquet"
def get_parquetpath(year): return "brfss/" + get_parquetfile(year)


spark = SparkSession \
  .builder\
  .appName("BRFSS")\
  .getOrCreate()
  
year=2015

## Setup the DataRobot project data 
# Read the parquetfile into a dataframe

pfile=get_parquetpath(year)
df = spark.read.parquet(pfile)

# Condition the CATHM1 so that we only look at no/yes (1,2) responses,
# and shift them to 0,1
corrected_df = df.\
  filter("CASTHM1 in (1,2)").\
  withColumn("Label", df.CASTHM1 - 1).\
  drop("CASTHM1").\
  withColumnRenamed("RFHLTH","ingoodhealth").\
  withColumnRenamed("HCVU651","hascoverage").\
  withColumnRenamed("MICHD","hasheartproblem").\
  withColumnRenamed("DRDXAR1","hasarthritis").\
  withColumnRenamed("CVDINFR4","hadheartattack").\
  withColumnRenamed("CVDCRHD4","hadangina").\
  withColumnRenamed("RACEGR3","racegroup").\
  withColumnRenamed("AGE_G","agegroup").\
  withColumnRenamed("BMI5CAT","bmicategory").\
  withColumnRenamed("SMOKER3","smoker").\
  withColumnRenamed("EDUCAG", "education").\
  withColumnRenamed("INCOMG", "income").\
  withColumnRenamed("RENTHOM1", "home_owner").\
  withColumnRenamed("SEX", "sex").\
  withColumnRenamed("STATE", "state")

# Choose the train/test split for modeling


train_df,test_df = train_test_split(corrected_df.toPandas(), test_size=0.25)
  
your_token=os.environ["DR_API_TOKEN"]
dr.Client(token=your_token, endpoint='https://app.datarobot.com/api/v2')
project = dr.Project.start(train_df,
                           project_name='BRDSS',
                           target='Label',
                           metric='AUC',
                           worker_count=4)


## Working with Models 
project.wait_for_autopilot() #block until all models finish - takes about 27 mins!
models = project.get_models()

for idx, model in enumerate(models):
    print('[{}]: {} - {}'.
          format(idx, model.metrics['AUC']['validation'], model.model_type))
    
    
best_model = models[0] # choose best model - eXtreme Gradient Boosted Trees Classifier with Early Stopping (Fast Feature Binning)

try:
  feature_impacts = best_model.get_feature_impact() # if they've already been computed
except dr.errors.ClientError as e:
  assert e.status_code == 404 # if the feature impact score haven't been computed already
  impact_job = best_model.request_feature_impact()
  feature_impacts = impact_job.get_result_when_complete()
feature_impacts.sort(key=lambda x: x['impactNormalized'])
feature_impacts_df = pd.DataFrame(feature_impacts).set_index('featureName')
feature_impacts_df.impactNormalized.plot.bar()

## Make Predictions
#use the testing data to make predictions
prediction_data_in_dr = project.upload_dataset(test_df) # In most use cases, use new data for this
predict_job = best_model.request_predictions(prediction_data_in_dr.id)
predictions = predict_job.get_result_when_complete()
print(predictions.head(25)) 

## ROC and Confusion
model = best_model
roc = model.get_roc_curve('validation')


df = pd.DataFrame(roc.roc_points)

threshold = roc.get_best_f1_threshold()

metrics = roc.estimate_threshold(threshold)
metrics

roc_df = pd.DataFrame({
    'Predicted Negative': [metrics['true_negative_score'],
                           metrics['false_negative_score'],
                           metrics['true_negative_score'] + metrics[
                               'false_negative_score']],
    'Predicted Positive': [metrics['false_positive_score'],
                           metrics['true_positive_score'],
                           metrics['true_positive_score'] + metrics[
                               'false_positive_score']],
    'Total': [metrics['true_negative_score'] + metrics['false_positive_score'],
              metrics['false_negative_score'] + metrics['true_positive_score'],
              metrics['true_negative_score'] + metrics['false_positive_score']+
              metrics['false_negative_score'] + metrics['true_positive_score']]})
roc_df.index = pd.MultiIndex.from_tuples([
    ('Actual', '-'), ('Actual', '+'), ('Total', '')])
roc_df.columns = pd.MultiIndex.from_tuples([
    ('Predicted', '-'), ('Predicted', '+'), ('Total', '')])
ignore = roc_df.style.set_properties(**{'text-align': 'right'})
roc_df


## ROC Curve
dr_dark_blue = '#08233F'
dr_roc_green = '#03c75f'
white = '#ffffff'
dr_purple = '#65147D'
dr_dense_green = '#018f4f'


def pfun():
  fig = plt.figure(figsize=(8, 8))
  axes = fig.add_subplot(1, 1, 1, facecolor=dr_dark_blue)
  plt.scatter(df.false_positive_rate, df.true_positive_rate, color=dr_roc_green)
  plt.plot(df.false_positive_rate, df.true_positive_rate, color=dr_roc_green)
  plt.plot([0, 1], [0, 1], color=white, alpha=0.25)
  plt.title('ROC curve')
  plt.xlabel('False Positive Rate (Fallout)')
  plt.xlim([0, 1])
  plt.ylabel('True Positive Rate (Sensitivity)')
  plt.ylim([0, 1])

pfun()