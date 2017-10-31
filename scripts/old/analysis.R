library(sparklyr)
library(dplyr)

sc <- spark_connect(master = "yarn-client")

get_file <- function(year) { spark_read_parquet(sc, paste0("brfss",year), paste0("brfss/",year,".parquet")) %>% na.omit() %>% mutate(year=year)}
                                                
brfss_2011 <- get_file("2011")
brfss_2012 <- get_file("2012")
brfss_2013 <- get_file("2013")
brfss_2014 <- get_file("2014")
brfss_2015 <- get_file("2015")
#brfss <- rbind(brfss_2011, brfss_2012, brfss_2013, brfss_2014, brfss_2015) %>%
brfss <- brfss_2011 %>%
# Encode the values as strings so that the prediction system will perform one-hot encoding with no further effort
filter(CASTHM1 %in% c(1,2) ) %>%
mutate(ingoodhealth = if_else(RFHLTH == 1, 'yes', if_else(RFHLTH == 2, 'no', 'unknown'))) %>%
mutate(hascoverage = if_else(HCVU651 == 1, 'yes', if_else(HCVU651 == 2, 'no', 'unknown'))) %>%
mutate(hasheartproblem = if_else(MICHD == 1, 'yes', if_else(MICHD == 2, 'no', 'unknown'))) %>%
mutate(hasasthma = if_else(CASTHM1 == 1, 0, 1)) %>%
mutate(hasarthritis = if_else(DRDXAR1 == 1, 'yes', if_else(DRDXAR1 == 2, 'no', 'unknown'))) %>%
mutate(hadheartattack = if_else(CVDINFR4 == 1, 'yes', if_else(CVDINFR4 == 2, 'no', 'unknown'))) %>%
mutate(hadangina = if_else(CVDCRHD4 == 1, 'yes', if_else(CVDCRHD4 == 2, 'no', 'unknown'))) %>%
mutate(racegroup = if_else(RACEGR3 == 1, 'whitenonhispanic', if_else(RACEGR3 == 2, 'blacknonhispanic', if_else(RACEGR3 == 3, 'othernonhispanic', if_else(RACEGR3 == 4, 'multiracialnonhispanic', if_else(RACEGR3 == 5, 'hispanic', 'unknown' )))))) %>%
mutate(agegroup = if_else(AGE_G == 1, '18', if_else(AGE_G == 2, '25', if_else(AGE_G == 3, '35', if_else(AGE_G == 4, '45', if_else(AGE_G == 5, '55', if_else(AGE_G == 6, '65', 'unknown' ))))))) %>%
mutate(bmicategory = if_else(BMI5CAT == 1, 'underweight', if_else(BMI5CAT == 2, 'normalweight', if_else(BMI5CAT == 3, 'overweight', if_else(BMI5CAT == 4, 'obese',  'unknown' ))))) %>%
mutate(smoker = if_else(SMOKER3 == 1, 'everyday', if_else(SMOKER3 == 2, 'somedays', if_else(SMOKER3 == 3, 'former', if_else(SMOKER3 == 4, 'never', 'unknown' )))))




response <- 'hasasthma'

features <- c(
'ingoodhealth',
'hascoverage',
'hasheartproblem',
'hasarthritis',
'racegroup',
'agegroup',
'bmicategory',
'smoker',
'hadheartattack',
'hadangina')

# We will use a simple function to evaluate the various models given the test data.
model_eval <- function(model, test) {
  prediction <- sdf_predict(model, test)
#  accuracy <- ml_classification_eval(prediction, 'hasasthma', 'prediction', metric='accuracy')
    accuracy <- ml_binary_classification_eval(prediction, 'hasasthma', 'prediction')

  print(paste0("accuracy = ",round(accuracy,2)))
  truth <- select(prediction,hasasthma) %>% collect
  p <- select(prediction, prediction)  %>% collect
  table(p$prediction,truth$hasasthma)
}


# We'll partition the data set 75%/25% between training and testing sets
partitions <- sdf_partition(brfss, training=0.75, test=0.25, seed=1234567)
sdf_register(partitions, c("brfss_training", "brfss_testing"))
brfss_training <- tbl(sc,"brfss_training")
brfss_testing <- tbl(sc, "brfss_testing")
        
# We then evaluate multiple different models
model_eval(ml_logistic_regression(brfss_training, response=response, features=features), brfss_testing) 
model_eval(ml_decision_tree(brfss_training, response=response, features=features, type='classification'), brfss_testing)
model_eval(ml_gradient_boosted_trees(brfss_training, response=response, features=features, type='classification'), brfss_testing)
model_eval(ml_random_forest(brfss_training, response=response, features=features, type='classification'), brfss_testing)

# All of these models have the same accuracy of approx 91% - and this simply reflects the underlying 
# data skew by predicting 'no asthma' for every example!


# We will reconstruct the dataset and use undersampling to try to get a more nuanced model
brfss_no_asthma <- brfss %>% filter(hasasthma==0) %>% sdf_sample(0.4, replacement=TRUE, seed=12345)
brfss_with_asthma <- brfss %>% filter(hasasthma==1)
brfss <- rbind(brfss_no_asthma, brfss_with_asthma)

partitions <- sdf_partition(brfss, training=0.75, test=0.25, seed=1234567)
sdf_register(partitions, c("brfss_training", "brfss_testing"))
brfss_training <- tbl(sc,"brfss_training")
brfss_testing <- tbl(sc, "brfss_testing")

# We'll reevaluate the models
model_eval(ml_logistic_regression(brfss_training, response=response, features=features), brfss_testing) 
model_eval(ml_decision_tree(brfss_training, response=response, features=features, type='classification'), brfss_testing)
model_eval(ml_gradient_boosted_trees(brfss_training, response=response, features=features, type='classification'), brfss_testing)
model_eval(ml_random_forest(brfss_training, response=response, features=features, type='classification'), brfss_testing)


# This gets only an accuracy of 80%, with high sensitivity (~99%), and low specifity (~4.5%)
# So it rarely marks a false positive, but will very frequently mark a false negative. 

