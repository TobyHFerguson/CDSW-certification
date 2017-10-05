#Packages <- c("caret", "e1071")
#install.packages("e1071")

library(sparklyr)
library(dplyr)
library(caret)

## Basic data
# We create a data frame and then 'score' the data frame using a simple algorithm

seed <- 8393

set.seed(seed)
df <- data.frame(x1 = c(rep('a',8), rep('b', 8)), 
                 x2 = sample(c('a','b'), replace=TRUE, size = 16), 
                 x3 = sample(c('a','b'), replace=TRUE, size=16))
data <- df %>% 
  mutate(y = if_else(x1=='a' | (x2 == 'b' & x3 == 'b'), 1L, sample(0:1L, 1))) %>% 
  select(y,x1,x2,x3) 
data

## ML Workflow
#* Create training/testing partitions
inTrain <- createDataPartition(factor(data$y), p=0.75, list=FALSE)
training <- data[inTrain,] %>% mutate_if(is.numeric,factor)
testing <- data[-inTrain,] %>% mutate_if(is.numeric,factor)
testing
training

# 1. Create a model

model <- train(y ~ ., data=training, method="glm")
model

# 1. Evaluate the model

train_prediction <- predict(object = model, training)
confusionMatrix(train_prediction, training$y)

# 1. Predict and evaluate on the test data

test_prediction <- predict(object = model, testing)
confusionMatrix(test_prediction, testing$y)

## Dummy variables
#In the above system we allowed the underlying model generation to construct dummy variables. Here we'll do it explicitly.

#We regenerate the data, this time leaving the response as a numeric to make dummy variable processing easier

set.seed(seed)
#data <- df %>% 
#  mutate(y = if_else(x1=='a' | (x2 == 'b' & x3 == 'b'), 1L, sample(0:1L, 1))) %>% 
#  select(y,x1,x2,x3)

data_dummy <- data.frame(predict(dummyVars(~ . , data=data, fullRank = T), newdata=data))
data_dummy$y <- as.factor(data_dummy$y)
data_dummy <- data_dummy %>% mutate_if(is.double, as.integer) 
data_dummy

# Next create the test and training sets using this new data

#inTrain <- createDataPartition(data_dummy$y, p=0.75, list=FALSE)
training_dummy <- data_dummy[inTrain,]
testing_dummy <- data_dummy[-inTrain,]
training_dummy
testing_dummy

# 1. Create a model

model_dummy <- train(y ~ ., data=training_dummy, method="glm")
model_dummy

# 1. Evaluate the model

training_dummy_prediction <- predict(object = model_dummy, training_dummy)
confusionMatrix(training_dummy_prediction, training_dummy$y)

# 1. Predict and evaluate

testing_dummy_prediction <- predict(object = model_dummy, testing_dummy)
confusionMatrix(testing_dummy_prediction, testing_dummy$y)

