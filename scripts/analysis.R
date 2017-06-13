library(sparklyr)
library(dplyr)

sc <- spark_connect(master = "yarn-client")

brfss_2011 <- spark_read_parquet(sc, "brfss_2011", "brfss/2011.parquet") %>% na.omit()
brfss_2012 <- spark_read_parquet(sc, "brfss_2012", "brfss/2012.parquet") %>% na.omit()
brfss <- union_all(brfss_2011,brfss_2012) %>% 
  filter(CASTHM1 != 9) %>%
  mutate(RESPONSE = ifelse(CASTHM1 == 1, 0, 1)) %>% 
  select(-CASTHM1)

brfss.training <- sdf_sample(brfss, 0.6, seed = 1234567)
brfss.test <- sdf_sample(brfss, 0.4, seed = 1234567)
model <- ml_logistic_regression(x=brfss.training, RESPONSE ~ . - DISPCODE)
summary(model)
prediction <- sdf_predict(model, brfss.test) %>% collect()