## review failures file. 

## the left-out from entering into the input DB.
library(here)
source(here("Automation/00_Functions_automation.R"))
library(tidyverse)
library(lubridate)
library(purrr)
library(readxl)
library(tidytext)
library(writexl)
library(data.table)

FailuresDB <- fread("N:/COVerAGE-DB/Data/inputDB_failures.csv") %>% 
  mutate(Date = as.Date(Date, format = "%d.%m.%Y"))


#| Some Notes on the Failures DB:
#| Austria has duplicates in 20-11-2022 vaccination data: done
#| Canada shows duplicate but it is not significant; 10-06-2022 when we don't have data for cases
#| 

firstlook <- FailuresDB %>% 
  count(Country, Measure, reason) 


FailuresDB %>% 
  filter(Country == "Czechia", reason == "duplicate") %>% 
  count(Date, Measure) |> 
  View()
#  count(Region, Code, Measure)
# # distinct(Date) %>% 
#  count(templateID, reason) %>% 
#  View()



FailuresDB %>% 
  filter(Country == "China") %>% 
  #distinct(Date) %>% 
  # count(Region, Code) %>% 
  View()


FailuresDB %>% 
  filter(Country == "New Zealand") %>%
  pull(Age) %>% 
  '['(1) %>% '=='("0")

#distinct(Measure, reason)

FailuresDB %>% 
  filter(Country == "France", Measure == "Cases") %>% 
  count(Region, reason) %>% View()
count(reason)
#  count(reason)
# distinct(Date)
summarise(min_date = min(Date),
          max_date = max(Date)) %>% 
  View()


#################### Czechia issue

inputDB |> 
  filter(Country == "Czechia",
         Measure == "Vaccination1") |> View()

## The issue of duplicate Czechia, which I never get from where it happens. 

inputDB <- fread("N:/COVerAGE-DB/Data/inputDB_internal.csv") %>% 
  mutate(Date = as.Date(Date, format = "%d.%m.%Y")) 

Czechia <- readRDS("N:/COVerAGE-DB/Automation/Hydra/Czechia.rds") |> 
  mutate(Date = as.Date(Date, format = "%d.%m.%Y"))

Czechia <- Czechia |> 
  unique()

Czechia_binded <- Czechia |> 
  mutate(templateID = "CZ")


all_out <- inputDB |> 
  bind_rows(Czechia_binded) |> 
  unique()

all_out2 <- all_out |> 
  mutate(Date = ddmmyyyy(Date)) |> 
  # Sort data
  arrange(Country,
          Date,
          Region,
          Code,
          Sex, 
          Measure,
          Metric,
          suppressWarnings(as.integer(Age))) %>% 
  # Drop extra date variable
  select(Country, Region, Code,  Date, Sex, Age, AgeInt, Metric, Measure, Value, templateID)


# all_out2 |> 
#   filter(Country == "Czechia",
#          Measure == "Vaccination1") |> View()


all_out2 |> 
  fwrite("N:/COVerAGE-DB/Data/inputDB_internal.csv")

