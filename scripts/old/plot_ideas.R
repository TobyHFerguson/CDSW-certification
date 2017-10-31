## APPENDIX


# Function for pulling out relevant statistics
#

stats <- function(tbl, source, dest, labels) {
  tbl  %>%
  select_(source, "hasasthma") %>% 
  collect %>% 
  mutate_(dest = if_else(is.null(source), 'unknown', labels[[source]])) %>%
  mutate_all(funs(as.factor(.))) #%>%
#  mutate_at(vars(dest), funs(ordered(., levels=unlist(labels)))) %>%
#  pgraph(dest)
}

l = list('everyday', 'somedays', 'former', 'never')
l[[9]] <- 'unknown'


stats(brfss, "SMOKER3", "smoker", l)
str(x)




## Prevalence of Asthma in various classes
# What is being shown here is the percentage of some group that have asthma, broken down
# by the sub-groups. 


# Function for determining prevalence of a factor class against overall population and of the asthmatics within the class
prevalence <- function(column, tbl) {
  tbl_grp <- tbl %>% group_by_(column)
  class_pop_t <- tbl_grp %>% summarize(class_pop=n())
  asthmatic_grp <- tbl_grp %>% filter(hasasthma=='yes')%>%summarize(asthmatic=n())
  j <- full_join(asthmatic_grp, class_pop_t, by=column) %>% 
    mutate(asthmatic_in_class = asthmatic/class_pop, 
           class_in_total_pop = class_pop/nrow(tbl)) %>% 
    select(column,asthmatic_in_class, class_in_total_pop)
  melt(data.frame(j))

ggplot(data = prevalence("smoker", group)) +
  geom_bar(mapping = aes(x = smoker, y = round(value*100,2), fill=variable), 
           position="dodge", 
           stat = "identity") 

# What the above diagram shows is that some 50% of the total population has never smoked and, in that group
# a little less than 10% have asthma, whereas around 5% of the population smoke on some days, but a little more than
# 10% of them have asthma. There's a slight correlation between amount of smoking and the percentage of asthmatic
# suffers in that (sub) population, but not much.

#ggplot(data = prevalence("racegroup", group)) +
#  geom_bar(mapping = aes(x = racegroup, y = round(prev*100,2)), stat = "identity") + labs(y="prevalence") 

#


## Evaluate
# This draws a basic count plot:
brfss_smoking <- brfss  %>%
  select(SMOKER3, hasasthma) %>% 
  collect %>% 
  mutate(smoker = if_else(SMOKER3 == 1, 'everyday', if_else(SMOKER3 == 2, 'somedays', if_else(SMOKER3 == 3, 'former', if_else(SMOKER3 == 4, 'never', 'unknown' ))))) %>%
  mutate_all(funs(as.factor(.))) %>%
  mutate_at(vars(smoker), funs(ordered(., levels=c("never", "former", "somedays", "everyday", "unknown"))))

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


############
p <- brfss_smoking %>% ggplot(aes(x=smoker, y=(..count../sum(..count..)*100))) + labs(y="prevalence") + geom_bar(aes(fill=hasasthma))

p + stat_count(geom="text", 
               colour="black", 
               size=3.5,
               aes(x=smoker, 
                   label=..count.., group=hasasthma), 
                   position=position_stack(vjust=0.5))

# Now to label each value - this from https://stackoverflow.com/questions/30057765/histogram-ggplot-show-count-label-for-each-bin-for-each-category

brfss_smoking %>% ggplot(aes(x=smoker)) + labs(y="prevalence") + geom_bar(aes(fill=hasasthma, y=(..count../sum(..count..)*100))) + 
stat_count(geom="text", 
               colour="black", 
               size=3.5,
               aes(x=smoker, 
                   y=(..count../sum(..count..)*100),
                   label=paste0(round(..count../sum(..count..)*100, 2), "%"), group=hasasthma), 
                   position=position_stack(vjust=0.5)) +
    stat_count(geom="text", 
                   colour="blue", 
                   size=3.5,
                   aes(x=smoker, 
                       label=paste0(round(..count../sum(..count..)*100, 2), "%")))
##########


pgraph(brfss_smoking, "smoker")

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

