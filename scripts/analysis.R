library(sparklyr)
library(dplyr)

sc <- spark_connect(master = "yarn-client")

get_file <- function(year) { spark_read_parquet(sc, paste0("brfss",year), paste0("brfss/",year,".parquet")) %>% na.omit() }
                                                
brfss_2011 <- get_file("2011")
#brfss_2012 <- get_file("2012")
#brfss_2013 <- get_file("2013")
#brfss_2014 <- get_file("2014")
#brfss_2015 <- get_file("2015")
#brfss <- union_all(brfss_2011, brfss_2012, brfss_2013, brfss_2014, brfss_2015) %>%
brfss <- brfss_2011 %>%
#Ensure that the null replies are omitted
  filter(CASTHM1 != 9) %>%
#Ensure that the 'no' responses are coded as '0', and everything else as a '1'
  mutate(RESPONSE = ifelse(CASTHM1 == 1, 0L, 1L)) %>%
# Delete the CASTHM1 and DISPCODE columns
select(-CASTHM1) %>%
select(-DISPCODE)


partitions <- sdf_partition(brfss, training=0.75, test=0.25, seed=1234567)
sdf_register(partitions, c("brfss_training", "brfss_testing"))
brfss_training <- tbl(sc,"brfss_training")
brfss_testing <- tbl(sc, "brfss_testing")
brfss_model <- ml_logistic_regression(partitions$training, RESPONSE ~ .)

brfss_predict <- sdf_predict(brfss_model, brfss_testing) %>% collect

marked <- sdf_predict(model, partitions$test) %>% mutate(good = RESPONSE == prediction)  %>% collect()
mean(marked$good)
