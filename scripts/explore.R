library(sparklyr)
library(dplyr)
library(ggplot2)

sc <- spark_connect(master = "yarn-client")

get_file <- function(year) { spark_read_parquet(sc, paste0("brfss",year), paste0("brfss/",year,".parquet")) %>% na.omit() %>% mutate(year=year)}
                                                
brfss_2011 <- get_file("2011")
brfss_2012 <- get_file("2012")
brfss_2013 <- get_file("2013")
brfss_2014 <- get_file("2014")
brfss_2015 <- get_file("2015")
#brfss <- rbind(brfss_2011, brfss_2012, brfss_2013, brfss_2014, brfss_2015) %>%
brfss <- brfss_2011

# Filter out the missing or unknown asthmatic responses

brfss <- brfss %>% filter(CASTHM1 %in% c(1,2)) %>% mutate(hasasthma=CASTHM1 - 1) %>% mutate(smoker = if_else(SMOKER3 == 1, 'everyday', if_else(SMOKER3 == 2, 'somedays', if_else(SMOKER3 == 3, 'former', if_else(SMOKER3 == 4, 'never', 'unknown' )))))


brfss_smoking <- select(brfss, smoker, hasasthma) %>% collect %>% mutate_all(funs(as.factor(.)))

# This draws a basic count plot:
brfss_smoking %>% ggplot(aes(x=smoker)) + geom_bar()

# What I want is to have a plot that
# * shows the density for each label against the asthmatic and non-asthmatic population


p <- brfss_smoking %>% ggplot(aes(x=smoker, y=..count../sum(..count..), group=hasasthma)) + labs(y="density") + geom_bar(aes(y=..count../sum(..count..),fill=hasasthma))

p

# Now to label each value - this from https://stackoverflow.com/questions/30057765/histogram-ggplot-show-count-label-for-each-bin-for-each-category

p + stat_count(geom="text", 
               colour="black", 
               size=3.5,
               aes(x=smoker, label=round(..count../sum(..count..), 2), group=hasasthma), position=position_stack(vjust=0.5))

