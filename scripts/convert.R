## All urls of of the format https://www.cdc.gov/brfss/annual_data/${YEAR}/files/LLCP${YEAR}XPT.zip
## ALl files are of the format LLCP${YEAR}.XPT

years <- c(2011:2015)

## foreign package provides the states variable, which can be used to map from state fips codes to names
require(foreign)

require(dplyr)

states <- read.csv("../data/states.csv", header=TRUE)

get_asthma_data_for_year <- function(yr) {
    ## Read in the data
    file <- paste0("../data/LLCP",yr,".XPT")
    data <- tbl_df(read.xport(file))
    ## Add the asthmatic state and year then strip to get only the data we want
    mutate(data, ASTHMATIC = ASTHMA3 == 1 & ASTHNOW == 1, 
               Year=yr) %>%
        select(Year, X_STATE, ASTHMATIC)

}

data <- bind_rows(lapply(X=years, FUN=get_asthma_data_for_year))

## Calculate and plot the rate of asthma by state. By 'rate of asthma' we're meaning
## what percentage of the population in the state reported as being asthmatic
summarized <- summarize(group_by(data, Year, X_STATE), Asthma.Density=mean(ASTHMATIC))
summarized <- mutate(summarized,State.Name=factor(X_STATE, levels=states$fips, labels=states$state))

names(summarized) <- c("Year", "State.Code", "Asthma.Density", "State.Name")
## names(summarized)
## [1] "Year"           "State.Code"     "Asthma.Density" "State.Name"    

g <- ggplot(data=summarized, aes(x=Year, y=Asthma.Density, group=1))
g+facet_wrap(~State.Name)+geom_point()+geom_smooth(method = "lm")





## Getting pm data
read_pm25_for_year <- function(yr) {
    file <- paste0("../data/annual_all_",yr,".csv")
    pm25 <- tbl_df(read.csv(file, header=TRUE))
    filter(pm25, Parameter.Code==88502, Completeness.Indicator=="Y") %>% select(Year,State.Code, Arithmetic.Mean, State.Name)
}

## pm25 contains the pm25 data for the years
pm25 <- bind_rows(lapply(X=years, FUN=read_pm25_for_year))

## Summarize by year and by state, taking the mean of the readings in each state.
## Not at all certain that this is meaningful. It might be better simply to take the max
## and see how that works. 
pm25.summarized <- summarize(group_by(pm25, Year, State.Code),PM25=mean(Arithmetic.Mean))
pm25.summarized <- mutate(pm25.summarized,State.Name=factor(State.Code, levels=states$fips, labels=states$state))
##  names(pm25)
## [1] "Year"            "State.Code"      "Arithmetic.Mean" "State.Name" 


## Join the asthma and the pm25 by year and state code
## 
ij <- inner_join(summarized, pm25.summarized)

ijc <- gather(ij, measure, value, Asthma.Density, PM25, convert=TRUE)

g <- ggplot(ijc, aes(x=Year, y=value, group=1 ), color=measure)
g+facet_wrap(~State.Name)+geom_point()+geom_line()

g <- ggplot(pm25.summarized, aes(x=Year, y=PM25, group=1))
g+facet_wrap(~State.Name)+geom_point()+geom_line()+labs(title="PM2.5")
