#Estonia vaccine 

library(here)
source(here("Automation/00_Functions_automation.R"))
library(lubridate)
library(dplyr)
library(tidyverse)

# assigning Drive credentials in the case the script is verified manually  
if (!"email" %in% ls()){
  email <- "maxi.s.kniffka@gmail.com"
}


# info country and N drive address

ctr          <- "Estonia_vaccine" # it's a placeholder
dir_n        <- "N:/COVerAGE-DB/Automation/Hydra/"



# Drive credentials
drive_auth(email = Sys.getenv("email"))
gs4_auth(email = Sys.getenv("email"))

# Drive urls
# rubric <- get_input_rubric() %>% filter(Short == "EE")
# 
# ss_i <- rubric %>% 
#   dplyr::pull(Sheet)
# 
# ss_db <- rubric %>% 
#   dplyr::pull(Source)
# 
# # reading data from Drive and last date entered 
# 
# In_drive <- get_country_inputDB("EE")%>% 
#   select(-Short)
# #Save entered case and test data  
# 
# drive_archive= In_drive%>%
#   filter(Measure== "Cases"| Measure== "Tests")


#read in vaccine data 

## Source: https://opendata.digilugu.ee/docs/#/

In_vaccine= read.csv("https://opendata.digilugu.ee/covid19/vaccination/v2/opendata_covid19_vaccination_location_county_agegroup_gender.csv")


#process 

In_vaccine[In_vaccine == ""] <- NA
In_vaccine[In_vaccine == " "] <- NA

Out_vaccine= In_vaccine%>%
  select(Date= ï..StatisticsDate, Age=AgeGroup, Sex= Gender, Value=TotalCount,Measure= MeasurementType, Region= LocationCounty) %>% 
  filter(Measure != "DosesAdministered") %>% 
  mutate(Sex = case_when(
    is.na(Sex)~ "UNK",
    Sex == "Male" ~ "m",
    Sex == "Female" ~ "f"))%>%
  mutate(Age = case_when(
    is.na(Age) ~ "UNK",
    TRUE~ as.character(Age)))%>%
  mutate(Region = case_when(
    is.na(Region) ~ "UNK",
    TRUE~ as.character(Region)))%>%
  mutate(Age=recode(Age, 
                    `0-4`="0",
                    `5-11`="5",
                    `12-15`="12",
                    `16-17`="16",
                    `18-29`="18",
                    `30-39`="30",
                    `40-49`="40",
                    `50-59`="50",
                    `60-69`="60",
                    `70-79`="70",
                    `80+`="80",
                    `Unknown`="UNK"))%>% 
  mutate(Measure= recode(Measure,
                         `Vaccinated`="Vaccination1",
                         `FullyVaccinated`="Vaccination2"))%>%
  mutate(AgeInt = case_when(
    Age == "0" ~ 5L,
    Age == "5" ~ 7L,
    Age == "12" ~ 4L,
    Age == "16" ~ 2L,
    Age == "18" ~ 12L,
    Age == "80" ~ 25L,
    Age == "UNK" ~ NA_integer_,
    TRUE ~ 10L))%>% 
  mutate(Metric = "Count") %>% 
  mutate(
    Date = ymd(Date),
    Date = paste(sprintf("%02d",day(Date)),    
                 sprintf("%02d",month(Date)),  
                 year(Date),sep="."),
    Country = "Estonia")%>% 
  mutate(Code = case_when(
    Region == "Harju maakond" ~ "EE-37",
    Region == "Hiiu maakond" ~ "EE-39",
    Region == "Ida-Viru maakond" ~ "EE-45",
    Region == "JÃ¤rva maakond" ~ "EE-52",
    Region == "JÃµgeva maakond" ~ "EE-50",
    Region == "LÃ¤Ã¤ne-Viru maakond" ~ "EE-60",
    Region == "LÃ¤Ã¤ne maakond" ~ "EE-56",
    Region == "PÃ¤rnu maakond" ~ "EE-68",
    Region == "PÃµlva maakond" ~ "EE-64",
    Region == "Rapla maakond" ~ "EE-71",
    Region == "Saare maakond" ~ "EE-74",
    Region == "Tartu maakond" ~ "EE-79",
    Region == "VÃµru maakond" ~ "EE-87",
    Region == "Valga maakond" ~ "EE-81",
    Region == "Viljandi maakond" ~ "EE-84",
    Region == "UNK" ~ "EE-UNK+"
  )) %>% 
  select(Country, Region, Code, Date, Sex, 
         Age, AgeInt, Metric, Measure, Value)

##totals
Out_vaccineALL <- Out_vaccine %>% 
  group_by(Country, Date, Sex, Age, AgeInt, Metric, Measure) %>% 
  summarise(Value = sum(Value)) %>% 
  mutate(Region = "All",
         Code = paste0("EE"))
    
#put together with archived data 

# Out= rbind(drive_archive,Out_vaccine)
#     
# write_sheet(Out, 
#             ss = ss_i, 
#             sheet = "database")
Out_vaccine <- bind_rows(Out_vaccine, Out_vaccineALL)
Out_vaccine <- Out_vaccine %>% 
  sort_input_data()

write_rds(Out_vaccine, paste0(dir_n, ctr, ".rds"))

#log_update(pp = ctr, N = nrow(Out_vaccine))


# ------------------------------------------
# now archive

data_source <- paste0(dir_n, "Data_sources/", ctr, "/vaccine_age_",today(), ".csv")

write_csv(In_vaccine, data_source)

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

##############################################################################




#esto <- read_rds(paste0(dir_n, ctr, ".rds"))

















