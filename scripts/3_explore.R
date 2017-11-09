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



# Function for showing asthma prevalence as percentage of global population
global_prev <- function(tbl, column, title="") {
  ggplot(tbl, 
         aes_string(x=column, 
                    y="(..count../sum(..count..))")) +
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
               aes(label=paste0(round(..count../sum(..count..)*100, 2), "%"))) +
  labs(title=title, y='Asthma prevalence as percent of global population')
}

# Function for determining asthma prevalence within classes of a factor column
asthma_prevalence_within_class <- function(tbl, column) {
  # Get the counts in each class of the given column
  counts <- tbl %>% group_by_(column) %>% count() %>% dplyr::rename(pop=n)
  asthmatics <- tbl %>% 
    group_by_(column) %>% 
    filter(hasasthma=="yes") %>% 
    count() %>%
    inner_join(counts, by=column) %>%
    mutate(prev=n/pop) %>%
    select_(column, "prev")
  asthmatics
}

# Function for plotting asthma prevalence within classes
in_class_prev <- function(tbl,column, title="") {
  a <- asthma_prevalence_within_class(tbl,column)
  ggplot(data=a,aes_string(x=column, y="prev*100")) + 
  geom_bar(stat="identity") +
  geom_label(aes(label=round(prev*100,2)),position=position_stack(vjust=0.8))+
  labs(title=title, y="Prevalance of Asthma within class (%)", x=paste0("class: ",column))
}

# Function to wrap up the other graphs
# This just kept printing the second graph!
prev <- function(tbl, column, title="") {
  print(global_prev(tbl, column, paste0(title," (global)")))
  print(in_class_prev(tbl, column, paste0(title," (in class)")))
}

### Heart Attack sufferers
b <- brfss %>%
  select(MICHD, hasasthma) %>%
  mutate(hadheartattack = if_else(MICHD == 1, 'yes', if_else(MICHD == 2, 'no', 'unknown'))) %>%
  select(hasasthma,hadheartattack) %>%
  collect %>%
  mutate_all(funs(as.factor(.))) %>%
  mutate_at(vars(hadheartattack), funs(ordered(., levels=c("no", "yes",  "unknown"))))
  global_prev(b, 'hadheartattack', "Asthma and Heart Attacks")
  in_class_prev(b, 'hadheartattack', "Asthma Prevalence amongst Heart Attack Suffers")


### Race
b <- brfss  %>%
  select(RACEGR3, hasasthma) %>% 
  mutate(racegroup = if_else(RACEGR3 == 1, 'whitenonhispanic', if_else(RACEGR3 == 2, 'blacknonhispanic', if_else(RACEGR3 == 3, 'othernonhispanic', if_else(RACEGR3 == 4, 'multiracialnonhispanic', if_else(RACEGR3 == 5, 'hispanic', 'unknown' )))))) %>%
  select(racegroup, hasasthma) %>%
  collect %>% 
  mutate_all(funs(as.factor(.))) 
  global_prev(b, 'racegroup', "Asthma and Race")
  in_class_pre(b, 'racegroup', "Asthma and Race")

### Health
b <- brfss %>%
  select(RFHLTH, hasasthma) %>%
  mutate(ingoodhealth = if_else(RFHLTH == 1, 'yes', if_else(RFHLTH == 2, 'no', 'unknown'))) %>%
  select(ingoodhealth, hasasthma) %>%
  collect %>%
  mutate_all(funs(as.factor(.))) %>%
  mutate_at(vars(ingoodhealth), funs(ordered(., levels=c("no", "yes",  "unknown"))))
  global_prev(b,'ingoodhealth', "Asthma and Health")
  in_class_prev(b,'ingoodhealth', "Asthma and Health")

### Has Health care coverage
b <- brfss %>%
  select(HCVU651, hasasthma) %>%
  mutate(hascoverage = if_else(HCVU651 == 1, 'yes', if_else(HCVU651 == 2, 'no', 'unknown'))) %>%
  select(hascoverage, hasasthma) %>%
  collect %>%
  mutate_all(funs(as.factor(.))) %>%
  mutate_at(vars(hascoverage), funs(ordered(., levels=c("no", "yes",  "unknown"))))
  global_prev(b,'hascoverage', "Asthma and Health care coverage")
  in_class_prev(b,'hascoverage', "Asthma and Health care coverage")

### Has Arthritis
b<-  brfss %>%
  select(DRDXAR1, hasasthma) %>%
  mutate(hasarthritis = if_else(DRDXAR1 == 1, 'yes', if_else(DRDXAR1 == 2, 'no', 'unknown'))) %>%
  select(hasarthritis, hasasthma) %>%
  collect %>%
  mutate_all(funs(as.factor(.))) %>%
  mutate_at(vars(hasarthritis), funs(ordered(., levels=c("no", "yes",  "unknown"))))
  global_prev(b, 'hasarthritis', "Asthma and Arthritis")
  in_class_prev(b, 'hasarthritis', "Asthma and Arthritis")

# Asthma affects arthritis suffers at twice the rate of non-arthritis suffers

### Rent or own home

b <- brfss %>%
  select(RENTHOM1, hasasthma) %>%
  mutate(renthome = if_else(RENTHOM1 == 1, 'own', if_else(RENTHOM1 == 2, 'rent', if_else(RENTHOM1 == 3, 'other', 'unknown')))) %>%
  select(renthome, hasasthma) %>%
  collect %>%
  mutate_all(funs(as.factor(.))) %>%
  mutate_at(vars(renthome), funs(ordered(., levels=c("no", "yes",  'other', "unknown"))))
  global_prev(b, 'renthome', "Asthma and Home Ownership")
  in_class_prev(b, 'renthome', "Asthma and Home Ownership")

### Sex

b <-  brfss %>%
  select(SEX, hasasthma) %>%
  mutate(sexx = if_else(SEX == 1, 'male', if_else(SEX == 2, 'female', 'unknown'))) %>%
  select(sex = sexx, c("hasasthma")) %>%
  collect %>%
  mutate_all(funs(as.factor(.))) %>%
  mutate_at(vars(sex), funs(ordered(., levels=c("male", "female",  "unknown")))) 
  global_prev(b, 'sex', 'Asthma and Sex')
  in_class_prev(b, 'sex', 'Asthma and Sex')

### Age Group

b <- brfss %>%
  select(AGE_G, hasasthma) %>%
  mutate(agegroup = if_else(AGE_G == 1, '18', if_else(AGE_G == 2, '25', if_else(AGE_G == 3, '35', if_else(AGE_G == 4, '45', if_else(AGE_G == 5, '55', if_else(AGE_G == 6, '65', 'unknown' ))))))) %>%
  select(agegroup, hasasthma) %>%
  collect %>%
  mutate_all(funs(as.factor(.))) %>%
  mutate_at(vars(agegroup), funs(ordered(., levels=c("18", "25",  '35', '45', '55', '65', "unknown"))))
  global_prev(b, 'agegroup', 'Asthma and Age')
  in_class_prev(b, 'agegroup', 'Asthma and Age')

### Weight

b <- brfss %>%
  select(BMI5CAT, hasasthma) %>%
  mutate(bmicategory = if_else(BMI5CAT == 1, 'underweight', if_else(BMI5CAT == 2, 'normalweight', if_else(BMI5CAT == 3, 'overweight', if_else(BMI5CAT == 4, 'obese',  'unknown'))))) %>%
  select(bmicategory, hasasthma) %>%
  collect %>%
  mutate_all(funs(as.factor(.))) %>%
  mutate_at(vars(bmicategory), funs(ordered(., levels=c('underweight', 'normalweight', 'overweight', 'obese', "unknown"))))
  global_prev(b, 'bmicategory', "Asthma and Weight")
  in_class_prev(b, 'bmicategory', "Asthma and Weight")

### Smokers

b <-brfss  %>%
  select(SMOKER3, hasasthma) %>% 
  mutate(smoker = if_else(SMOKER3 == 1, 'everyday', if_else(SMOKER3 == 2, 'somedays', if_else(SMOKER3 == 3, 'former', if_else(SMOKER3 == 4, 'never', 'unknown' ))))) %>%
  select(smoker, hasasthma) %>%
  collect %>%
  mutate_all(funs(as.factor(.))) %>%
  mutate_at(vars(smoker), funs(ordered(., levels=c("never", "former", "somedays", "everyday", "unknown")))) 
  global_prev(b, 'smoker', "Asthma and Smoking")
  in_class_prev(b, 'smoker', "Asthma and Smoking")

### Educational Level

b <- brfss  %>%
  select(EDUCAG, hasasthma) %>% 
  mutate(education = if_else(EDUCAG == 1, 'non hs grad', if_else(EDUCAG == 2, 'hs grad', if_else(EDUCAG == 3, 'college', if_else(EDUCAG == 4, 'college grad', 'unknown'))))) %>%
  select(education, hasasthma) %>%
  collect %>%
  mutate_all(funs(as.factor(.))) %>%
  mutate_at(vars(education), funs(ordered(., levels=c("non hs grad", "hs grad", "college", "college grad", "unknown"))))
  global_prev(b, 'education', "Asthma and Education")
  in_class_prev(b, 'education', "Asthma and Education")

### Income group

l = list('10k', "15k", "20k", "25k", "35k", "50k", "75k", ">75k")
l[[77]] <- 'unknown'
l[[99]] <- 'refused'
b <- brfss  %>%
  select(INCOMG, hasasthma) %>% 
  mutate(income = if_else(INCOMG==1, "10k",
                         if_else(INCOMG==2, "15k",
                                if_else(INCOMG==3, "20k",
                                       if_else(INCOMG==4, "25k",
                                              if_else(INCOMG==5,"35k",
                                                     if_else(INCOMG==6,"50k",
                                                            if_else(INCOMG==7, "75k",
                                                                   if_else(INCOMG==8,">75k",
                                                                          if_else(INCOMG==99, "refused", "unknown")))))))))) %>%
  select(income, hasasthma) %>%
  collect %>%
  mutate_all(funs(as.factor(.))) %>%
  mutate_at(vars(income), funs(ordered(., levels=unlist(l))))
  global_prev(b, 'income', "Asthma and Income")
  in_class_prev(b, 'income', "Asthma and Income")

### State
b <- brfss  %>%
  select(STATE, hasasthma) %>% 
  collect %>%
  mutate_all(funs(as.factor(.)))
  global_prev(b, 'STATE', "Asthma and Location (state)")
  in_class_prev(b, 'STATE', "Asthma and Location (state)")


