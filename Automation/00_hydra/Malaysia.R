# Created on: 27.06.2022

## README: Malaysia MOH is publishing the linelists in parquet format; 
## read_parquet is not reading URLs yet, so we will download the file first
## and then read using arrow::read_parquet()

library(here)
source(here("Automation/00_Functions_automation.R"))

# assigning Drive credentials in the case the script is verified manually  
if (!"email" %in% ls()){
  email <- "mumanal.k@gmail.com"
}

# info country and N drive address
ctr <- "Malaysia"
dir_n <- "N:/COVerAGE-DB/Automation/Hydra/"

# Drive credentials

drive_auth(email = Sys.getenv("email"))
gs4_auth(email = Sys.getenv("email"))


## SOURCE: Malaysia MOH GITHUB repo: https://github.com/MoH-Malaysia/covid19-public
###################################

# GEO PARAMETERS #
state_dist <- read.csv("https://raw.githubusercontent.com/MoH-Malaysia/covid19-public/main/epidemic/linelist/param_geo.csv") %>% 
  dplyr::distinct(state, idxs)

# CASES & DEATHS ===============
##' MK, 17.07.2024: links FOR CASES AND DEATHS ARE NOT WORKING ANYMORE. 
##' So, we change the cases from linelist 1-year age to 5-year age interval data.
##' Keep deaths from original file 

## SOURCE: https://github.com/MoH-Malaysia/covid19-public/tree/main/epidemic/linelist

# CASES: LINELIST # ================

#cases_url <- "https://moh-malaysia-covid19.s3.ap-southeast-1.amazonaws.com/linelist_cases.parquet"
cases_url <- "https://raw.githubusercontent.com/MoH-Malaysia/covid19-public/main/epidemic/cases_state.csv"
cases_source <- paste0(dir_n, "Data_sources/", ctr, "/", ctr, "-cases_linelist_",today(), ".parquet")

 
## DOWNLOAD CASES AND READ IN THE DATA ##

download.file(cases_url,
              destfile = cases_source,
              mode = "wb")

#cases_linelist <- arrow::read_parquet(cases_source) 

cases_linelist <- read.csv(cases_source)


## WRNAGLING, ETC ##

## Purpose is to convert the linelist dataset into cumulative time series dataset, so:

cases_linelist2 <- cases_linelist %>% 
  dplyr::select(date, state, cases_0_4, cases_5_11,
                cases_12_17, cases_18_29, cases_30_39, cases_40_49,
                cases_50_59, cases_60_69, cases_70_79, cases_80) |> 
  tidyr::pivot_longer(cols = -c("date", "state"),
                      names_to = "Cases_age",
                      values_to = "Value") |> 
  tidyr::separate(col = Cases_age, into = c("cases", "age", "age1"), sep = "_") |> 
  dplyr::select(-c("cases", "age1")) |> 
  dplyr::mutate(Age = age,
                Date = ymd(date)) %>% 
  dplyr::select(Date, Age, state, Value) %>% 
  dplyr::group_by(state, Age) %>% 
  dplyr::arrange(Date) |> 
  dplyr::mutate(Value = cumsum(Value),
                AgeInt = case_when(Age == 0 ~ 5,
                                   Age == 5 ~ 7,
                                   Age == 12 ~ 6,
                                   Age == 18 ~ 12,
                                   Age == 80 ~ 25,
                                   TRUE ~ 10),
                Sex = "b") %>% 
  dplyr::ungroup()
  
  
cases <- cases_linelist2 %>% 
  dplyr::left_join(state_dist, by = c("state")) %>% 
  dplyr::mutate(Country = "Malaysia",
                Code = paste0("MY-", 
                              str_pad(idxs, width=2, side="left", pad="0")),
                Metric = "Count",
                Measure = "Cases") %>% 
  dplyr::select(Country, Region = state, Code, 
                Date, Sex, Age, AgeInt, 
                Metric, Measure, Value) |> 
  dplyr::mutate(Date = ddmmyyyy(Date)) |> 
  sort_input_data()
  
  
  
  
# DEATHS ===================

## The deprecated file has the data in 1-year age for cases, deaths and data on vaccination. 
MalaysiaDeaths <- readRDS("N:/COVerAGE-DB/Automation/Hydra/deprecated/Malaysia.rds") |> 
  filter(Measure == "Deaths")


epiData_malaysia <- dplyr::bind_rows(cases, MalaysiaDeaths) 


# Vaccination =======================================

# Vaccination data are available by age and by sex, separately, 
# and each has state (& district) level.
# we will use the age-specific data only and identify sex as 'b' for both

## Documentation: https://github.com/MoH-Malaysia/covid19-public/tree/main/vaccination

Vacc_age <- read.csv("https://github.com/MoH-Malaysia/covid19-public/raw/main/vaccination/vax_demog_age.csv") %>% 
  dplyr::select(-district)

Age_Vacc <- Vacc_age %>% 
  ## These data were by district, we have to sum value then at the level of States,
  dplyr::group_by(date, state) %>% 
  dplyr::summarise(across(.cols = everything(), ~ sum(.x))) %>% 
  dplyr::ungroup() %>% 
  # then: mutate & cumsum at the state level
  dplyr::group_by(state) %>% 
  dplyr::arrange(date) %>% 
  dplyr::mutate(across(.cols = -c("date"), ~ cumsum(.x))) %>% 
  tidyr::pivot_longer(cols = -c("date", "state"),
                      names_to = c("Measure"),
                      values_to = "Value")  %>% 
  dplyr::mutate(Measure = str_replace(Measure, "_", " ")) %>% 
  tidyr::separate(Measure,
                  into = c("Measure", "Age"),
                  sep = " ",
                  convert = TRUE) %>% 
  dplyr::mutate(date = ymd(date),
                Age = str_replace(Age, "_", "-"),
                Age = case_when(Age == "missing" ~ "UNK",
                                Age == "5-11" ~ "5",
                                Age == "12-17" ~ "12",
                                Age == "18-29" ~ "18",
                                Age == "30-39" ~ "30",
                                Age == "40-49" ~ "40",
                                Age == "50-59" ~ "50",
                                Age == "60-69" ~ "60",
                                Age == "70-79" ~ "70",
                                Age == "80" ~ "80"),
                AgeInt = case_when(
                  Age == "5" ~ 7L,
                  Age == "12" ~ 6L,
                  Age == "18" ~ 12L,
                  Age == "80" ~ 25L,
                  Age == "UNK" ~ NA_integer_,
                  TRUE ~ 10L),
                Measure = case_when(Measure == "partial" ~ "Vaccination1",
                                    Measure == "full" ~ "Vaccination2",
                                    Measure == "booster" ~ "Vaccination3",
                                    Measure == "booster2" ~ "Vaccination4",
                                    TRUE ~ Measure)) %>% 
  dplyr::select(Date = date, Region = state,
               Age, AgeInt, Measure, Value) %>% 
  dplyr::left_join(state_dist, by = c("Region" = "state")) %>% 
  dplyr::mutate(Country = "Malaysia",
                Code = paste0("MY-", 
                              str_pad(idxs, width=2, side="left", pad="0")),
                Metric = "Count",
                Sex = "b")  %>% 
  dplyr::select(Country, Region, Code, 
                Date, Age, AgeInt, 
                Sex, Metric, Measure, Value) %>% 
  dplyr::mutate(Date = ddmmyyyy(Date)) %>% 
  sort_input_data()

## binding epi-data & vaccination data into one df

out <- epiData_malaysia %>% 
  dplyr::bind_rows(Age_Vacc) 


############################
#### Saving data in N: ####
###########################
write_rds(out, paste0(dir_n, ctr, ".rds"))

## Updating Hydra :)

log_update(pp = ctr, N = nrow(out))


#END

# LINELIST # ================

#deaths_url <- "https://moh-malaysia-covid19.s3.ap-southeast-1.amazonaws.com/linelist_deaths.parquet"

# deaths_source <- paste0(dir_n, "Data_sources/", ctr, "/", ctr, "-deaths_linelist_",today(), ".parquet")
# DOWNLOAD CASES AND READ IN THE DATA ##
# 
# download.file(deaths_url,
#               destfile = deaths_source,
#               mode = "wb")
# 
# deaths_linelist <- arrow::read_parquet(deaths_source)  
# 
# ## WRNAGLING, ETC ##
# 
# deaths_linelist2 <- deaths_linelist %>% 
#   dplyr::mutate(age = as.integer(age),
#                 male = as.character(male)) %>% 
#   dplyr::mutate(Date = ymd(date),
#                 Age = case_when(age < 0 ~ "UNK",
#                                 age > 105 ~ "105",
#                                 TRUE ~ as.character(age)),
#                 Sex = case_when(male == "1" ~ "m", 
#                                 TRUE ~ "f")) %>%  
#   dplyr::group_by(Date, Age, Sex, Region = state) %>%
#   dplyr::count(name = "Value") %>% 
#   dplyr::ungroup() %>% 
#   tidyr::complete(Sex, Age, Region, Date=dates_f, fill=list(Value=0)) %>% 
#   dplyr::group_by(Age, Sex, Region) %>% 
#   dplyr::mutate(Value = cumsum(Value)) %>% 
#   dplyr::ungroup()
# 
# 
# deaths <- deaths_linelist2 %>% 
#   dplyr::left_join(state_dist, by = c("Region" = "state")) %>% 
#   dplyr::mutate(Country = "Malaysia",
#                 AgeInt = case_when(Age == "UNK" ~ NA_integer_,
#                                    TRUE ~ 1L),
#                 Code = paste0("MY-", 
#                               str_pad(idxs, width=2, side="left", pad="0")),
#                 Metric = "Count",
#                 Measure = "Deaths")  %>% 
#   dplyr::select(Country, Region, Code, 
#                 Date, Sex, Age, AgeInt, 
#                 Metric, Measure, Value)
