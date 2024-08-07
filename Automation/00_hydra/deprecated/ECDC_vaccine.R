library(here)
source(here("Automation/00_Functions_automation.R"))
library(lubridate)
library(ISOweek)
library(aweek)


# assigning Drive credentials in the case the script is verified manually  
if (!"email" %in% ls()){
  email <- "jessica_d.1994@yahoo.de"
}


# info country and N drive address
ctr          <- "ECDC_vaccine"
dir_n <- "N:/COVerAGE-DB/Automation/Hydra/"

# Drive credentials
drive_auth(email = Sys.getenv("email"))
gs4_auth(email = Sys.getenv("email"))

## MK 26.01.2024: THE DATASET IS ARCVHIVED AS PER THE WEBSITE. 

## SourceWebsite <- "https://www.ecdc.europa.eu/en/publications-data/data-covid-19-vaccination-eu-eea"


#Read in data 

In <- read.csv("https://opendata.ecdc.europa.eu/covid19/vaccine_tracker/csv/data.csv")

#process

age_groups <- c("0", "5", "10", "15", "18", "25", "50", "60", "70", "80")

In_processed <- In %>%
  #select countries we need 
  dplyr::filter(
    Region %in% c("BG","CY","HR","HU",
                  #"IE",
                  "LU","MT","RO","PL","EL",
                  "PT","NO","NL","LV","SI","SK")) %>%
  select(YearWeekISO, 
         Vaccination1= FirstDose, 
         Vaccination2= SecondDose, 
         Vaccination3= DoseAdditional1, 
         Vaccination4 = DoseAdditional2, 
         Vaccination5 = DoseAdditional3,
         Vaccinations = UnknownDose,
         Code=Region,TargetGroup) %>%
  #remove category medical personnel and long term care residents 
  subset(TargetGroup != "HCW") %>%
  subset(TargetGroup != "LTCF")%>%
  #remove age groups that only separate above and below 60
  subset(TargetGroup != "1_Age60+") %>%
  subset(TargetGroup != "1_Age<60")%>%
  subset(TargetGroup != "Age<18")%>%
  pivot_longer(!YearWeekISO & !Code & !TargetGroup, names_to= "Measure", values_to= "Value")%>%
  #data was given separate by vaccine brand, sum those together 
  group_by(YearWeekISO, Code,TargetGroup, Measure) %>% 
  mutate(Value = sum(Value),
         YearWeekISO = case_when(YearWeekISO == "2023-09" ~ "2023-W09",
                                 YearWeekISO == "2023-10" ~ "2023-W10",
                                 YearWeekISO == "2023-11" ~ "2023-W11",
                                 YearWeekISO == "2023-12" ~ "2023-W12",
                                 TRUE ~ YearWeekISO)) %>% 
  ungroup()%>% 
  distinct()%>% 
  mutate(YearWeekDay = paste(YearWeekISO, 5, sep = "-"),
        # Date = week2date(YearWeekDay),
         Date = ISOweek::ISOweek2date(YearWeekDay),
         Age = recode(TargetGroup, 
                     `ALL`= "TOT",
                     `Age0_4`= "0",
                     `Age5_9`= "5",
                     `Age10_14`= "10",
                     `Age15_17`= "15",
                     `Age18_24`= "18",
                     `Age25_49`="25",
                     `Age50_59`="50",
                     `Age60_69`="60",
                     `Age70_79`="70",
                     `Age80+`="80",
                     `AgeUNK`="UNK"),
         Country= recode(Code, 
                     `SI` = "Slovenia",
                     `SK` = "Slovakia",
                     `LV` = "Latvia",
                     `BG`= "Bulgaria",
                     `CY`= "Cyprus",
                     `HR`= "Croatia",
                     `HU`= "Hungary",
                    # `IE`= "Ireland",
                     `LU`= "Luxembourg",
                     `MT`="Malta",
                     `PL`="Poland",
                     `RO`="Romania",
                     `EL`="Greece",
                     `PT`="Portugal",
                     `NO`="Norway",
                     `NL` = "Netherlands")) 


## Expand the data based on Age Groups and CumSum

Out <- In_processed %>%
  select(Country, Code, Date, Age, Measure, Value) %>% 
  tidyr::complete(Age = age_groups, nesting(Country, Code, Date, Measure), fill=list(Value=0)) %>%  
  arrange(Date, Country, Code, Age, Measure) %>% 
  group_by(Country, Code, Age, Measure) %>% 
  mutate(Value = cumsum(Value)) %>% 
  ungroup() %>%
  mutate(
    Date = ymd(Date),
    Date = ddmmyyyy(Date),
    Code = case_when(Code == "EL" ~ "GR",
                     TRUE ~ Code),
    AgeInt = case_when(
               Age == "15" ~ 3L,
               Age == "18" ~ 7L,
               Age == "25" ~ 25L,
               Age == "50" ~ 10L,
               Age == "60" ~ 10L,
               Age == "70" ~ 10L,
               Age == "80" ~ 25L,
               Age == "UNK" ~ NA_integer_,
               Age == "TOT" ~ NA_integer_,
               TRUE ~ 5L),
    Metric = "Count",
    Sex= "b",
    Region="All")%>%
  select(Country, Region, Code, Date, Sex, 
         Age, AgeInt, Metric, Measure, Value) %>% 
  sort_input_data()

####we only need third vaccination for norway
## MK: 01.12.2022: I stopped this filter, we have data for Norway by Sex in Norway_Vaccine. 
## Here the data are for both sexes 'b'. I would keep having it as well. 
# Out <- Out %>% 
#   filter(Country != "Norway" | Measure != "Vaccination1") %>% 
#   filter(Country != "Norway" | Measure != "Vaccination2")



#save output data 
write_rds(Out, paste0(dir_n, ctr, ".rds"))
#log_update(pp = ctr, N = nrow(Out))

#zip input data

data_source <- paste0(dir_n, "Data_sources/", ctr, "/vaccine_age_",today(), ".csv")

write_csv(In, data_source)

zipname <- paste0(dir_n, 
                  "Data_sources/", 
                  ctr,
                  "/", 
                  ctr,
                  "_data_",
                  today(), 
                  ".zip")

zip::zipr(zipname, 
          data_source, 
          recurse = TRUE, 
          compression_level = 9,
          include_directories = TRUE)

file.remove(data_source)





