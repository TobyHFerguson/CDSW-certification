library(sparklyr)
library(dplyr)
library(ggplot2)

sc <- spark_connect(master = "yarn-client")

get_file <- function(year) { spark_read_parquet(sc, paste0("brfss",year), paste0("brfss/",year,".parquet")) %>% na.omit() %>% mutate(year=year)}
                                                
brfss_2011 <- get_file("2011")
#brfss_2012 <- get_file("2012")
#brfss_2013 <- get_file("2013")
#brfss_2014 <- get_file("2014")
#brfss_2015 <- get_file("2015")
#brfss <- rbind(brfss_2011, brfss_2012, brfss_2013, brfss_2014, brfss_2015) %>%
brfss <- brfss_2011

# Filter out the missing or unknown asthmatic responses

brfss <- brfss_2011 %>% filter(CASTHM1 %in% c(1,2)) %>% 
  mutate(hasasthma=if_else(CASTHM1 == 1, 'no', 'yes'))

prevalence <- function(column, tbl) {
  tbl <- tbl %>% group_by_(column)
  pop <- tbl %>% summarize(pop=n())
  asthmatic <- tbl %>% filter(hasasthma=='yes')%>%summarize(asthmatic=n())
  full_join(asthmatic, pop) %>% mutate(prev = asthmatic/pop) %>% select(column,prev)
}


## Prevalence of Asthma in various classes
# What is being shown here is the percentage of some group that have asthma, broken down
# by the sub-groups. 
### Smokers
# Within the group of smokers, what's the relative prevalence of asthma in the various sub-groups?
group <- brfss  %>%
  select(SMOKER3, hasasthma) %>% 
  collect %>% 
  mutate(smoker = if_else(SMOKER3 == 1, 'everyday', if_else(SMOKER3 == 2, 'somedays', if_else(SMOKER3 == 3, 'former', if_else(SMOKER3 == 4, 'never', 'unknown' ))))) %>%
  mutate_all(funs(as.factor(.))) %>%
  mutate_at(vars(smoker), funs(ordered(., levels=c("never", "former", "somedays", "everyday", "unknown"))))

ggplot(data = prevalence("smoker", group)) +
  geom_bar(mapping = aes(x = smoker, y = round(prev*100,2)), stat = "identity") + labs(y="prevalence")

### Heart Attack suffers
group <- brfss %>%
  select(CVDINFR4, hasasthma) %>%
  collect %>%
  mutate(hadheartattack = if_else(CVDINFR4 == 1, 'yes', if_else(CVDINFR4 == 2, 'no', 'unknown'))) %>%
  mutate_all(funs(as.factor(.))) %>%
  mutate_at(vars(hadheartattack), funs(ordered(., levels=c("no", "yes",  "unknown"))))

ggplot(data = prevalence("hadheartattack", group)) +
  geom_bar(mapping = aes(x = hadheartattack, y = round(prev*100,2)), stat = "identity") + labs(y="prevalence")

### Race
group <- brfss  %>%
  select(RACEGR3, hasasthma) %>% 
  collect %>% 
  mutate(racegroup = if_else(RACEGR3 == 1, 'whitenonhispanic', if_else(RACEGR3 == 2, 'blacknonhispanic', if_else(RACEGR3 == 3, 'othernonhispanic', if_else(RACEGR3 == 4, 'multiracialnonhispanic', if_else(RACEGR3 == 5, 'hispanic', 'unknown' )))))) %>%
  mutate_all(funs(as.factor(.)))

ggplot(data = prevalence("racegroup", group)) +
  geom_bar(mapping = aes(x = racegroup, y = round(prev*100,2)), stat = "identity") + labs(y="prevalence") 

It is clear that the multi-racial non-hispanic subgroup has a far larger share of asthmatics than any other racial group.


## Evaluate
# This draws a basic count plot:
brfss_smoking %>% ggplot(aes(x=smoker)) + geom_bar()

# What I want is to have a plot that
# * shows the density for each label against the asthmatic and non-asthmatic population


p <- brfss_smoking %>% ggplot(aes(x=smoker, y=(..count../sum(..count..)*100))) + labs(y="prevalence") + geom_bar(aes(fill=hasasthma))

p

p + stat_count(geom="text", 
               colour="black", 
               size=3.5,
               aes(x=smoker, 
                   label=..count.., group=hasasthma), 
                   position=position_stack(vjust=0.5))

# Now to label each value - this from https://stackoverflow.com/questions/30057765/histogram-ggplot-show-count-label-for-each-bin-for-each-category

p + stat_count(geom="text", 
               colour="black", 
               size=3.5,
               aes(x=smoker, 
                   label=paste0(round(..count../sum(..count..)*100, 2), "%"), group=hasasthma), 
                   position=position_stack(vjust=0.5)) +
    stat_count(geom="text", 
                   colour="blue", 
                   size=3.5,
                   aes(x=smoker, 
                       label=paste0(round(..count../sum(..count..)*100, 2), "%")))
# What we can see here is that smoking occurs (apart from the 'somedays' smokers)
a <- aes(x,y,label=n)
c <- brfss_smoking %>% ggplot() + geom_count(aes(x=smoker, y=hasasthma,color=..n.., size=..n..))
c <- c + guides(color="legend")
c+ geom_text(data = ggplot_build(c)$data[[1]], 
              a, nudge_y=0.1, color = "black")
c <- c + stat_count(geom="text", 
               colour="black", 
               size=3.5,
               aes(x=smoker, 
                   label=paste0(round(..count../sum(..count..)*100, 2), "%"), group=hasasthma), 
                   position=position_stack(vjust=0.5))
c

# What I want to do is to build a function that will produce these kind of graphs for my various categorical
# attributes.

# Might need to get the factorization to work properly, so more frequent etc. has a higher number.

# Get ideas for exploratory data science from hadley's R book (in bookmarks)

# I really want the prevalence of asthma in the given populations, by factor case:

