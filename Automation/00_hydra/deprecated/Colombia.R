library(here)
source(here("Automation/00_Functions_automation.R"))

# assigning Drive credentials in the case the script is verified manually  
if (!"email" %in% ls()){
  email <- "kikepaila@gmail.com"
}

# info country and N drive address
ctr <- "Colombia"
dir_n <- "N:/COVerAGE-DB/Automation/Hydra/"

# Drive credentials
drive_auth(email = Sys.getenv("email"))
gs4_auth(email = Sys.getenv("email"))

# Downloading data from the web
###############################
cases_url <- "https://www.datos.gov.co/api/views/gt2j-8ykr/rows.csv?accessType=DOWNLOAD"
tests_url <- "https://www.datos.gov.co/api/views/8835-5baf/rows.csv?accessType=DOWNLOAD"

# EA: the files became too big and the direct reading from the links is vary unstable this way.
# So better to download them first and read them directly from the local drive

# saving compressed data to N: drive
data_source_c <- paste0(dir_n, "Data_sources/", ctr, "/cases_",today(), ".csv")
data_source_t <- paste0(dir_n, "Data_sources/", ctr, "/tests_",today(), ".csv")


## MK, 06.07.2022: THE CASES file is too large, so will skip the temp file download and read it first and then save in data_source,

#download.file(cases_url, destfile = data_source_c)
download.file(tests_url, destfile = data_source_t)


# Loading data in the session
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~
# cases and deaths database
db <- read_csv(cases_url,
               locale = locale(encoding = "UTF-8"))

# tests database
db_m <- read_csv(data_source_t,
                 locale = locale(encoding = "UTF-8"))


# data transformation for COVerAGE-DB
#####################################

db2 <- db %>% 
  rename(Sex = Sexo,
         Region = 'Nombre departamento',
         City = 'Nombre municipio',
         status = 'Estado',
         unit = 'Unidad de medida de edad') %>% 
  mutate(Age = case_when(unit != 1 ~ 0,
                         unit == 1 & Edad <= 100 ~ Edad, 
                         unit == 1 & Edad > 100 ~ 100),
         Region = str_to_title(Region),
         Region = case_when(Region == "Sta Marta D.e." ~ "Santa Marta", 
                            TRUE ~ Region)) 

# unique(db2$Age)

cities <- c("MEDELLIN",
            "CALI")

db_city <- db2 %>% 
  filter(City %in% cities) %>% 
  mutate(Region = str_to_title(City))

db3 <- db2 %>% 
  bind_rows(db_city)

unique(db3$Age) %>% sort()

# cases ----------------------------------------------
# three dates for cases, preferred in this order: diagnosed, symptoms, reported to web
db_cases <- db3 %>% 
  rename(date_diag = 'Fecha de diagnóstico',
         date_repo = 'Fecha de notificación',
         date_sint = 'Fecha de inicio de síntomas') %>% 
  #separate(date_diag, c("date_diag", "trash1"), sep = " ") %>% 
  #separate(date_sint, c("date_sint", "trash2"), sep = " ") %>% 
  #separate(date_repo1, c("date_repo", "trash3"), sep = " ") %>% 
  mutate(date_f = case_when(!is.na(date_diag) ~ date_diag,
                            is.na(date_diag) & !is.na(date_sint) ~ date_sint,
                            is.na(date_diag) & is.na(date_sint) ~ date_repo),
         Measure = "Cases") %>% 
  mutate(date_f = as.character(date_f)) %>% 
  select(date_f, Age, Sex, Region, Measure)

# deaths -----------------------------------------------------------
db_deaths <- db3 %>% 
  filter(status == "Fallecido") %>% 
  rename(date = 'Fecha de muerte') %>% 
  separate(date, c("date_f", "trash1"), sep = " ") %>% 
  mutate(Measure = "Deaths") %>% 
  select(date_f, Age, Sex, Region, Measure)

# summarising new cases for each combination -----------------------
db4 <- bind_rows(db_cases, db_deaths) %>% 
  mutate(Sex = case_when(Sex == 'F' ~ 'f',
                         Sex == 'f' ~ 'f',
                         Sex == 'M' ~ 'm',
                         Sex == 'm' ~ 'm',
                         TRUE ~ 'UNK'),
         Age = as.character(Age)) %>% 
  group_by(Region, date_f, Measure, Sex, Age) %>% 
  summarise(new = n()) %>% 
  ungroup()
db4$date_f <- as.Date(db4$date_f)

# expanding the database to all possible combinations and cumulating values -------------
ages <- as.character(seq(0, 100, 1))
all_dates <- db4 %>% 
  filter(!is.na(date_f)) %>% 
  dplyr::pull(date_f)  
dates_f <- seq(min(all_dates), max(all_dates), by = '1 day')

db5 <- db4 %>% 
  tidyr::complete(Region, Measure, Sex, Age = ages, date_f = dates_f, fill = list(new = 0)) %>% 
  arrange(Region, Measure, Sex, suppressWarnings(as.integer(Age)),date_f) %>% 
  group_by(Region, Measure, Sex, Age) %>% 
  mutate(Value = cumsum(new)) %>% 
  select(-new) %>% 
  ungroup()

#######################
# template for database ------------------------------------------------------
#######################

# National data --------------------------------------------------------------
db_co <- db5 %>% 
  group_by(date_f, Sex, Age, Measure) %>% 
  summarise(Value = sum(Value), .groups= "drop") %>% 
  mutate(Region = "All")

# 5-year age intervals for regional data -------------------------------------
db_regions <- db5 %>% 
  mutate(Age = as.integer(Age),
         Age = Age - Age %% 5,
         Age = as.character(Age)) %>% 
  group_by(date_f, Region, Sex, Age, Measure) %>% 
  summarise(Value = sum(Value), .groups = "drop") %>% 
  arrange(date_f, Region, Measure, Sex, suppressWarnings(as.integer(Age))) 

# unique(db_regions$Region)

# merging national and regional data -----------------------------------
db_co_comp <- bind_rows(db_regions, db_co)

# summarising totals by age and sex in each date -----------------------------------
db_tot_age <- db_co_comp %>% 
  group_by(Region, date_f, Sex, Measure) %>% 
  summarise(Value = sum(Value), 
            .groups = "drop") %>% 
  mutate(Age = "TOT")

db_tot <- db_co_comp %>% 
  group_by(Region, date_f, Measure) %>% 
  summarise(Value = sum(Value), 
            .groups = "drop") %>% 
  mutate(Sex = "b",
         Age = "TOT")

db_inc <- db_tot %>% 
  filter(Measure == "Deaths",
         Value >= 10) %>% 
  group_by(Region) %>% 
  summarise(date_start = ymd(min(date_f)), 
            .groups = "drop") 

# appending all data in one database ----------------------------------------------
db_all <- bind_rows(db_co_comp, db_tot_age)

# filtering dates for each region (>50 deaths) -----------------------------------
db_all2 <- db_all %>% 
  left_join(db_inc, by = "Region") %>% 
  drop_na() %>% 
  filter((Region == "All" & date_f >= "2020-03-20") | date_f >= date_start) %>% 
  select(-date_start) %>% 
  bind_rows(db_tot)



# tests data -----------------------------------

db_inc2 <- db_inc %>% dplyr::pull(Region)

db_m_reg <- db_m %>% 
  mutate(date_sub = case_when(str_detect(Fecha, "/") ~ str_sub(Fecha, -10, -1),
                              TRUE ~ str_sub(Fecha, 1, 10)),
         date_sub = str_replace_all(date_sub, "/", "-")) %>% 
  mutate(date_f = lubridate::parse_date_time(date_sub, orders = c('ymd', 'dmy'))) %>% 
 # drop_na(date_f) %>% 
  rename(All = Acumuladas,
         t1 = 'Positivas acumuladas',
         t2 = "Negativas acumuladas",
         t3 = "Positividad acumulada",
         t4 = "Indeterminadas",
         t5 = "Procedencia desconocida") %>% 
  select(-c(Fecha, t1, t2, t3, t4, t5)) %>% 
  pivot_longer(cols = -c("date_f", "date_sub"), 
               names_to = "Region", 
               values_to = "Value") %>% 
  mutate(Measure = "Tests",
         Age = "TOT",
         Sex = "b",
         Region = case_when(Region == "Norte de Santander" ~ "Norte Santander",
                            Region == "Valle del Cauca" ~ "Valle",
                            Region == "Norte de Santander" ~ "Norte Santander",
                            TRUE ~ Region),
         Value = as.integer(Value)) %>% 
  filter(Region %in% db_inc2,
         date_f >= "2020-03-20") %>% 
  select(Region, date_f, Sex, Age, Measure, Value) %>% 
  drop_na()

unique(db_m_reg$Region) %>% sort()
unique(db_all2$Region) %>% sort()


# all data together in COVerAGE-DB format -----------------------------------
out <- db_all2 %>%
  bind_rows(db_m_reg) %>% 
  mutate(Country = "Colombia",
         AgeInt = case_when(Region == "All" & !(Age %in% c("TOT", "100")) ~ 1,
                            Region != "All" & !(Age %in% c("0", "1", "TOT")) ~ 5,
                            Region != "All" & Age == "0" ~ 5,
                           # Region != "All" & Age == "1" ~ 4,
                            Age == "100" ~ 5,
                            Age == "TOT" ~ NA_real_),
         Date = ddmmyyyy(date_f),
         Code = case_when(
           Region == "All" ~ "CO",
           Region == "Bogota" ~ "CO-DC",
           Region == "Amazonas" ~ "CO-AMA",
           Region == "Antioquia" ~ "CO-ANT",
           Region == "Arauca" ~ "CO-ARA",
           Region == "Atlantico" ~ "CO-ATL",
           Region == "Barranquilla" ~ "CO-BARRANQUILLA+",
           Region == "Bolivar" ~ "CO-BOL",
           Region == "Boyaca" ~ "CO-BOY",
           Region == "Caldas" ~ "CO-CAL",
           Region == "Cali" ~ "CO-CALI+",
           Region == "Caqueta" ~ "CO-CAQ",
           Region == "Cartagena" ~ "CO-CARTAGENA+",
           Region == "Casanare" ~ "CO-CAS",
           Region == "Cauca" ~ "CO-CAU",
           Region == "Cesar" ~ "CO-CES",
           Region == "Cordoba" ~ "CO-COR",
           Region == "Cundinamarca" ~ "CO-CUN",
           Region == "Choco" ~ "CO-CHO",
           Region == "Guainia" ~ "CO-GUA",
           Region == "Guaviare" ~ "CO-GUV",
           Region == "Huila" ~ "CO-HUI",
           Region == "Guajira" ~ "CO-LAG",
           Region == "Magdalena" ~ "CO-MAG",
           Region == "Medellin" ~ "CO-MEDELLIN+",
           Region == "Meta" ~ "CO-MET",
           Region == "Narino" ~ "CO-NAR",
           Region == "Norte Santander" ~ "CO-NSA",
           Region == "Putumayo" ~ "CO-PUT",
           Region == "Quindio" ~ "CO-QUI",
           Region == "Risaralda" ~ "CO-RIS",
           Region == "San Andres" ~ "CO-SAP",
           Region == "Santa Marta" ~ "CO-SANTA_MARTA+",
           Region == "Santander" ~ "CO-SAN",
           Region == "Sucre" ~ "CO-SUC",
           Region == "Tolima" ~ "CO-TOL",
           Region == "Valle" ~ "CO-VAC",
           Region == "Vaupes" ~ "CO-VAU",
           Region == "Vichada" ~ "CO-VID",
           TRUE ~ "CO-UNK+"),
         Metric = "Count") %>% 
  arrange(Region, date_f, Measure, Sex, suppressWarnings(as.integer(Age))) %>% 
  select(Country, Region, Code,  Date, Sex, Age, AgeInt, Metric, Measure, Value) %>% 
  sort_input_data()

unique(out$Region)
unique(out$Age)

############################################
#### saving database in N Drive ####
############################################
write_rds(out, paste0(dir_n, ctr, ".rds"))

# updating hydra dashboard
#log_update(pp = ctr, N = nrow(out))
#out %>% 
#  pull(Region) %>% unique()


####====================####

# compressing source files and cleaning stuff

write_csv(db, file = data_source_c)

data_source <- c(data_source_c, data_source_t)

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
