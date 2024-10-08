library(tidyverse)
library(stringi)

Sys.setenv(LANG = "en")
Sys.setlocale("LC_ALL","English")

fix_enc <- 
  function(db){
    db %>% 
      mutate(Region = ifelse(validUTF8(Region), Region, iconv(Region)),
             Region = stri_encode(Region, "", "UTF-8"))
  }

# out05 <- read.csv("N:/COVerAGE-DB/Data/Output_5_internal.csv", encoding = "UTF-8")
# out10 <- read.csv("N:/COVerAGE-DB/Data/Output_10_internal.csv", encoding = "UTF-8")

## Files with harmonized, fixed, and decumulated data
out05 <- read.csv("N:/COVerAGE-DB/Data/WebInputs/Output_5_fixed_website.csv", encoding = "UTF-8")
out10 <- read.csv("N:/COVerAGE-DB/Data/WebInputs/Output_10_fixed_website.csv", encoding = "UTF-8")
input <- read.csv("N:/COVerAGE-DB/Data/inputDB_internal.csv", encoding = "UTF-8")

out05_enc <- fix_enc(out05)
out10_enc <- fix_enc(out10)
input_enc <- fix_enc(input)

out05_enc_rd <- 
  out05_enc %>%
  select(Country, Region, Code, Date, Sex, Age, AgeInt, 
         Cases, Deaths, Tests, Vaccination1,
         Vaccination2, Vaccination3, Vaccination4, Vaccination5, Vaccination6, 
         VaccinationBooster, Vaccinations)
  # mutate(Cases = round(Cases, 1),
  #        Deaths = round(Deaths, 1),
  #        Tests = round(Tests, 1))

out10_enc_rd <- 
  out10_enc |> 
  select(Country, Region, Code, Date, Sex, Age, AgeInt, 
         Cases, Deaths, Tests, Vaccination1,
         Vaccination2, Vaccination3, Vaccination4, Vaccination5, Vaccination6, 
         VaccinationBooster, Vaccinations)
  # mutate(Cases = round(Cases, 1),
  #        Deaths = round(Deaths, 1),
  #        Tests = round(Tests, 1))

input_enc_rd <- 
  input_enc %>% 
  mutate(Value = round(Value, 1)) %>% 
  group_by(Country, Code, Region, Sex, Date, Age, AgeInt, Measure) %>%
  mutate(row_no = row_number()) %>%
  pivot_wider(names_from = "Measure", values_from = "Value") %>% 
  select(-row_no) |> 
  ungroup() |> 
 # select(-templateID) |> 
  select(Country, Region, Code, Date, Sex, Age, AgeInt, 
         Cases, Deaths, Tests, Vaccination1,
         Vaccination2, Vaccination3, Vaccination4, Vaccination5, Vaccination6, 
         VaccinationBooster, Vaccinations)

# write.csv(out05_enc, "N:/COVerAGE-DB/website_data/web_out05.csv", fileEncoding = "UTF-8")
# write.csv(out10_enc, "N:/COVerAGE-DB/website_data/web_out10.csv", fileEncoding = "UTF-8")
# write.csv(input_enc, "N:/COVerAGE-DB/website_data/web_input.csv", fileEncoding = "UTF-8")

write_csv(out05_enc_rd, "N:/COVerAGE-DB/website_data/web_out05.csv", 
          na = "")
write_csv(out10_enc_rd, "N:/COVerAGE-DB/website_data/web_out10.csv", 
          na = "")
write_csv(input_enc_rd, "N:/COVerAGE-DB/website_data/web_input.csv",
          na = "")

# write.csv(input_enc_rd, "N:/COVerAGE-DB/website_data/web_input.csv", 
#           fileEncoding = "UTF-8")

# ReadIn <- read.csv("N:/COVerAGE-DB/website_data/web_input.csv")

# writing a csv example to verify in Notepad++
test <-
  input_enc_rd %>% 
  #out05_enc_rd %>%
  #, "ES-C"
  filter(Code %in% c("DE-TH")) %>% 
  select(Country, Region, Code, Vaccination1, Date)

write.csv(test, "N:/COVerAGE-DB/website_data/web_test.csv", 
          fileEncoding = "UTF-8")
write_csv(test, "N:/COVerAGE-DB/website_data/web_test2.csv",
          na = "")
# 
# 
# # testing encoding issues (Spain and Germany)
# # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# original_regs <- 
#   out05 %>% 
#   filter(Code %in% c("DE-TH", "ES-C")) %>% 
#   pull(Region) %>% 
#   unique()
# 
# adjusted_regs <- 
#   out05_enc %>% 
#   filter(Code %in% c("DE-TH", "ES-C")) %>% 
#   pull(Region) %>% 
#   unique()
# 
# # all remaining issues
# enc_issues <- 
#   out05 %>% 
#   filter(!validUTF8(Region))
# 
# enc_issues <- 
#   out05_enc %>% 
#   filter(!validUTF8(Region)) %>% 
#   mutate(enc = stri_enc_mark(Region),
#          Region2 = stri_encode(Region, "", "UTF-8"),
#          enc2 = stri_enc_mark(Region2))
# 
# enc_issues <- 
#   out05 %>% 
#   filter(!validUTF8(Code))
