library(here)
source(here("Automation/00_Functions_automation.R"))

# assigning Drive credentials in the case the script is verified manually  
if (!"email" %in% ls()){
  email <- "kikepaila@gmail.com"
}

# info country and N drive address
ctr <- "CA_British_Columbia"
dir_n <- "N:/COVerAGE-DB/Automation/Hydra/"

# Drive credentials
drive_auth(email = Sys.getenv("email"))
gs4_auth(email = Sys.getenv("email"))


## MK: 13.07.2023, as announced on the website, data are not published anymore and the data available are until April 2023. 
## Decision: deprecated. 


# TR: major change 7 Feb 2022: Drive sheet is read separately 
# when the inputDB is compiled. If collection changes for 
# deaths in BC then we will rework it

# TR: pull urls from rubric instead 
# rubric_i <- get_input_rubric() %>% filter(Short == "CA_BC")
# ss_i     <- rubric_i %>% dplyr::pull(Sheet)
# 
# # loading deaths from Drive
# db_drive <- get_country_inputDB("CA_BC_d")
# 
# db_d <- db_drive %>% 
#   filter(Measure %in% c('Deaths','ASCFR')) %>% 
#   select(-Short)
# 
# write_sheet(db_d, ss = ss_i, sheet = "database")

#--------- input files -------------------------------------#
data_source <- paste0(dir_n, "Data_sources/", ctr, "/cases",today(), ".csv")

# case and death data
url <- "http://www.bccdc.ca/Health-Info-Site/Documents/BCCDC_COVID19_Dashboard_Case_Details.csv"

#-------saving input files locally 

download.file(url, destfile = data_source)
# loading data
#df <- read_csv(data_source)
df <- fread(data_source)

out <- 
  df %>% 
  mutate(Age = str_sub(Age_Group, 1, 2),
         Age = recode(Age,
                      "<1" = "0",
                      "90+" = "90",
                      "Un" = "UNK"),
         Sex = recode(Sex,
                      "M" = "m",
                      "F" = "f",
                      "U" = "UNK")) %>% 
  select(Date = Reported_Date,
         Sex, Age) %>% 
  mutate(Date = ymd(Date)) %>% 
  group_by(Date, Sex, Age) %>% 
  summarise(new = n()) %>% 
  ungroup() %>% 
  tidyr::complete(Sex, Age, Date, fill = list(new = 0)) %>% 
  arrange(Date) %>% 
  group_by(Sex, Age) %>% 
  mutate(Value = cumsum(new)) %>% 
  ungroup() %>% 
  mutate(Measure = "Cases",
         Country = "Canada",
         Region = "British Columbia",
         AgeInt = case_when(Age == "90" ~ 15,
                            Age == "UNK" ~ NA_real_,
                            TRUE ~ 10),
         Date = ddmmyyyy(Date),
         Code = "CA-BC",
         Metric = "Count") %>% 
  unique() %>% 
  # bind_rows(db_d) %>% # TR: removed 7-Feb 2022
  sort_input_data() 
  

# saving the csv
############################################
#### saving database in N Drive ####
############################################
write_rds(out, paste0(dir_n, ctr, ".rds"))

# updating hydra dashboard
#log_update(pp = ctr, N = nrow(out))



zipname <- paste0(dir_n, 
                  "Data_sources/", 
                  ctr,
                  "/", 
                  ctr,
                  "_data_",
                  today(), 
                  ".zip")

zipr(zipname, 
     data_source, 
     recurse = TRUE, 
     compression_level = 9,
     include_directories = TRUE)

# clean up file chaff
file.remove(data_source)
