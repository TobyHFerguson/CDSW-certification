library(sparklyr)
library(dplyr)
library(reshape)
library(ggplot2)


sc <- spark_connect(master = "yarn-client")

get_file <- function(year) { spark_read_parquet(sc, paste0("brfss",year), paste0("brfss/",year,".parquet")) %>% na.omit() %>% mutate(year=year)}
                                                
brfss_2011 <- get_file("2011")
brfss_2012 <- get_file("2012")
brfss_2013 <- get_file("2013")
brfss_2014 <- get_file("2014")
brfss_2015 <- get_file("2015")
brfss_2016 <- get_file("2016")
brfss <- rbind(brfss_2011, brfss_2012, brfss_2013, brfss_2014, brfss_2015) %>%
filter(CASTHM1 %in% c(1,2)) %>% 
  mutate(hasasthma=if_else(CASTHM1 == 1, 'no', 'yes'))

# Filter out the missing or unknown asthmatic responses

#brfss <- brfss_2011 %>% filter(CASTHM1 %in% c(1,2)) %>% 
#  mutate(hasasthma=if_else(CASTHM1 == 1, 'no', 'yes'))



# Function for showing prevalence of asthma as percentage of overall population within a class
pgraph <- function(tbl, column) {
  ggplot(tbl, 
         aes_string(x=column, 
                    y="(..count../sum(..count..))")) +
  labs(y='prevalence') + 
  geom_bar(aes(fill=hasasthma)) +
  stat_count(geom="text", 
             colour="black", 
             size=3.5,
             aes(label=paste0(round(..count../sum(..count..)*100, 2), "%"), 
                 group=hasasthma), 
             position=position_stack(vjust=0.5)) +
    stat_count(geom="text", 
               colour="blue", 
               size=3.5,
               aes(label=paste0(round(..count../sum(..count..)*100, 2), "%")))
}


### Heart Attack sufferers
 brfss %>%
  select(CVDINFR4, hasasthma) %>%
  collect %>%
  mutate(hadheartattack = if_else(CVDINFR4 == 1, 'yes', if_else(CVDINFR4 == 2, 'no', 'unknown'))) %>%
  mutate_all(funs(as.factor(.))) %>%
  mutate_at(vars(hadheartattack), funs(ordered(., levels=c("no", "yes",  "unknown")))) %>%
  pgraph('hadheartattack')

### Race
brfss  %>%
  select(RACEGR3, hasasthma) %>% 
  collect %>% 
  mutate(racegroup = if_else(RACEGR3 == 1, 'whitenonhispanic', if_else(RACEGR3 == 2, 'blacknonhispanic', if_else(RACEGR3 == 3, 'othernonhispanic', if_else(RACEGR3 == 4, 'multiracialnonhispanic', if_else(RACEGR3 == 5, 'hispanic', 'unknown' )))))) %>%
  mutate_all(funs(as.factor(.))) %>%
  pgraph('racegroup')

#It is clear that the multi-racial non-hispanic subgroup has a far larger share of asthmatics than any other racial group.

## Health
 brfss %>%
  select(RFHLTH, hasasthma) %>%
  collect %>%
  mutate(ingoodhealth = if_else(RFHLTH == 1, 'yes', if_else(RFHLTH == 2, 'no', 'unknown'))) %>%
  mutate_all(funs(as.factor(.))) %>%
  mutate_at(vars(ingoodhealth), funs(ordered(., levels=c("no", "yes",  "unknown")))) %>%
  pgraph('ingoodhealth')

## Has Health care coverage
 brfss %>%
  select(HCVU651, hasasthma) %>%
  collect %>%
  mutate(hascoverage = if_else(HCVU651 == 1, 'yes', if_else(HCVU651 == 2, 'no', 'unknown'))) %>%
  mutate_all(funs(as.factor(.))) %>%
  mutate_at(vars(hascoverage), funs(ordered(., levels=c("no", "yes",  "unknown")))) %>%
  pgraph('hascoverage')

## Has Arthritis
 brfss %>%
  select(DRDXAR1, hasasthma) %>%
  collect %>%
  mutate(hasarthritis = if_else(DRDXAR1 == 1, 'yes', if_else(DRDXAR1 == 2, 'no', 'unknown'))) %>%
  mutate_all(funs(as.factor(.))) %>%
  mutate_at(vars(hasarthritis), funs(ordered(., levels=c("no", "yes",  "unknown")))) %>%
  pgraph('hasarthritis')

## Had Angina

 brfss %>%
  select(CVDCRHD4, hasasthma) %>%
  collect %>%
  mutate(hadangina = if_else(CVDCRHD4 == 1, 'yes', if_else(CVDCRHD4 == 2, 'no', 'unknown'))) %>%
  mutate_all(funs(as.factor(.))) %>%
  mutate_at(vars(hadangina), funs(ordered(., levels=c("no", "yes",  "unknown")))) %>%
  pgraph('hadangina')

## Rent or own home

 brfss %>%
  select(RENTHOM1, hasasthma) %>%
  collect %>%
  mutate(renthome = if_else(RENTHOM1 == 1, 'own', if_else(RENTHOM1 == 2, 'rent', if_else(RENTHOM1 == 3, 'other', 'unknown')))) %>%
  mutate_all(funs(as.factor(.))) %>%
  mutate_at(vars(renthome), funs(ordered(., levels=c("no", "yes",  'other', "unknown")))) %>%
  pgraph('renthome')

## Sex

 brfss %>%
  select(SEX, hasasthma) %>%
  collect %>%
  mutate(sex = if_else(SEX == 1, 'male', if_else(SEX == 2, 'female', 'unknown'))) %>%
  mutate_all(funs(as.factor(.))) %>%
  mutate_at(vars(sex), funs(ordered(., levels=c("male", "female",  "unknown")))) %>%
  pgraph('sex')

## Age Group

 brfss %>%
  select(AGE_G, hasasthma) %>%
  collect %>%
  mutate(agegroup = if_else(AGE_G == 1, '18', if_else(AGE_G == 2, '25', if_else(AGE_G == 3, '35', if_else(AGE_G == 4, '45', if_else(AGE_G == 5, '55', if_else(AGE_G == 6, '65', 'unknown' ))))))) %>%
  mutate_all(funs(as.factor(.))) %>%
  mutate_at(vars(agegroup), funs(ordered(., levels=c("18", "25",  '35', '45', '55', '65', "unknown")))) %>%
  pgraph('agegroup')

## Weight

 brfss %>%
  select(BMI5CAT, hasasthma) %>%
  collect %>%
  mutate(bmicategory = if_else(BMI5CAT == 1, 'underweight', if_else(BMI5CAT == 2, 'normalweight', if_else(BMI5CAT == 3, 'overweight', if_else(BMI5CAT == 4, 'obese',  'unknown'))))) %>%
  mutate_all(funs(as.factor(.))) %>%
  mutate_at(vars(bmicategory), funs(ordered(., levels=c('underweight', 'normalweight', 'overweight', 'obese', "unknown")))) %>%
  pgraph('bmicategory')

## Smokers

brfss  %>%
  select(SMOKER3, hasasthma) %>% 
  collect %>% 
  mutate(smoker = if_else(SMOKER3 == 1, 'everyday', if_else(SMOKER3 == 2, 'somedays', if_else(SMOKER3 == 3, 'former', if_else(SMOKER3 == 4, 'never', 'unknown' ))))) %>%
  mutate_all(funs(as.factor(.))) %>%
  mutate_at(vars(smoker), funs(ordered(., levels=c("never", "former", "somedays", "everyday", "unknown")))) %>%
  pgraph('smoker')

## Educational Level

brfss  %>%
  select(EDUCAG, hasasthma) %>% 
  collect %>% 
  mutate(education = if_else(EDUCAG == 1, 'non hs grad', if_else(EDUCAG == 2, 'hs grad', if_else(EDUCAG == 3, 'college', if_else(EDUCAG == 4, 'college grad', 'unknown'))))) %>%
  mutate_all(funs(as.factor(.))) %>%
  mutate_at(vars(education), funs(ordered(., levels=c("non hs grad", "hs grad", "college", "college grad", "unknown")))) %>%
  pgraph('education')

## Income group

l = list('10k', "15k", "20k", "25k", "35k", "50k", "75k", ">75k")
l[[77]] <- 'unknown'
l[[99]] <- 'refused'
brfss  %>%
  select(INCOMG, hasasthma) %>% 
  collect %>% 
  mutate(income = if_else(INCOMG==1, "10k",
                         if_else(INCOMG==2, "15k",
                                if_else(INCOMG==3, "20k",
                                       if_else(INCOMG==4, "25k",
                                              if_else(INCOMG==5,"35k",
                                                     if_else(INCOMG==6,"50k",
                                                            if_else(INCOMG==7, "75k",
                                                                   if_else(INCOMG==8,">75k",
                                                                          if_else(INCOMG==99, "refused", "unknown")))))))))) %>%
  mutate_all(funs(as.factor(.))) %>%
  mutate_at(vars(income), funs(ordered(., levels=unlist(l)))) %>%
  pgraph('income')

## State
brfss  %>%
  select(STATE, hasasthma) %>% 
  collect %>%
  mutate_all(funs(as.factor(.))) %>%
  pgraph('STATE')


