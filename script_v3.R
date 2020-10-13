###-----------------------------###
### COVID data validation script

#------------ 1. Installing necessary packages ----------------#
pkgs <- c("readxl", "tidyverse", "writexl", "janitor", "httr", "jsonlite", "lubridate", "here")

new.pkgs <- pkgs[!(pkgs %in% installed.packages()[,"Package"])]
if(length(new.pkgs)) install.packages(new.pkgs)
lapply(pkgs, require, character.only = TRUE)

# work directory
setwd(Sys.glob("C:\\Users\\*\\World Health Organization\\COVID Intelligence - Documents\\Scripts\\Data validation"))

# Country name dictionary
dictionary <- read_excel(".\\..\\..\\Master Data\\DVA_COVID-19_data - v2.xlsx", sheet = "dictionary")

dictionary$original_spell_up <- toupper(dictionary$original_spell)


#------------ 2. Load data ----------------#

# 1) Specify what would be the reference data for each region
path_afro = "xmart" # or "draft\\00_latest_validation_file\\afro.csv"
path_paho = "xmart" # or "draft\\00_latest_validation_file\\paho.csv"
path_searo = "xmart" # or "draft\\00_latest_validation_file\\searo.csv"
path_wpro = "xmart" # or "draft\\00_latest_validation_file\\wpro.csv"
path_other = "xmart" # International conveyance cases
path_emro = "xmart" # or "draft\\00_latest_validation_file\\emro.xlsx"
path_euro = "xmart" # or "draft\\00_latest_validation_file\\qry_COVID_cases_by_date_final.CSV"

path_territory = "00_latest_validation_file\\territory.xlsx"

#path_sitrep_today = "draft\\00_latest_validation_file\\sitrep_today.xlsx"
#path_sitrep_yesterday = "draft\\00_latest_validation_file\\sitrep_yesterday.xlsx"


cat("\n\n\nSelect the sitrep table from Today\n\n\n")
file_sitrep_today <- read_excel(choose.files(default = paste("Validation Files", sep = ""), 
                                             caption = paste("Select the file containing today's sitrep table", sep = ""), multi = F))

cat("\n\n\nSelect the sitrep table from yesterday.\n\n\n")
file_sitrep_yesterday <- read_excel(choose.files(default = paste("Validation Files", sep = ""), 
                                                 caption = paste("Select the file containing yesterday's sitrep table", sep = ""), multi = F))



# Create a copy of these files in a date specific subfolder for historical record
fdate <- format(file.mtime(path_territory), "%Y-%m-%d") 
dir.create(file.path("00_latest_validation_file\\archive", fdate), showWarnings = FALSE)

current_files <- list.files(path="00_latest_validation_file", pattern="xlsx|XLSX|csv|CSV", full.names = TRUE)
file.copy(current_files, paste0("00_latest_validation_file\\archive\\", fdate), overwrite=TRUE)



# 2) Original scraped data from XMART (same as merged CSV)
pacman::p_load(httpuv, AzureAuth, httr, jsonlite)

tok0 <- get_azure_token(resource = "712b0d0d-f9c5-4b7a-80d6-8a83ee014bca", 
                        tenant = "f610c0b7-bd24-4b39-810b-3dc280afb590", 
                        app = "712b0d0d-f9c5-4b7a-80d6-8a83ee014bca",
                        auth_type="authorization_code",
                        password = "qQKa]APZ_0q.OwO.Oq1H3ndnFNsa16u7",
                        use_cache=TRUE)

list_azure_tokens()

access_token <- tok0$credentials$access_token
bearer <- paste("Bearer", access_token)
print(bearer)

headers <- add_headers(Authorization = bearer)

response <- GET("https://extranet.who.int/xmart-api/odata/ncov/V_DAILY_CLEAN_ADMIN0_XLS", headers)
#response <- GET("https://portal-uat.who.int/xmart-api/odata/NCOV/V_DAILY_CLEAN_ADMIN0_XLS", headers)
json_xmart_pull <- content(response, "text")
xmart_pull <- as_tibble(fromJSON(json_xmart_pull)$value)

names(dictionary)

# See if all country names are in the dictionary
xmart_pull <-
  xmart_pull %>%
  left_join(dictionary, by=c("country_name"="original_spell")) %>%
  #filter(is.na(correct_spell_intel)) %>%
  select(names(xmart_pull), correct_spell_intel, ISO_3_CODE, WHO_CODE) %>%
  mutate(country_name=correct_spell_intel)
  



# 3) read data according to the source specified
regions <- c("afro", "paho", "emro", "euro", "searo", "wpro", "territory", "other") #, "sitrep_today", "sitrep_yesterday")


# Read data for each region
for (reg in regions) {
  
  reg_up = toupper(reg)
  path_name = get(paste("path_", reg, sep=""))
  file_name = paste("file_", reg, sep="")
  
  if (path_name=="xmart") {
    
      assign(file_name, xmart_pull %>% filter(region == toupper(reg)))
    
  } else if (grepl('.xlsx', path_name)) {
    
      assign(file_name, read_excel(path_name))
           
  } else if (grepl('.CSV', path_name)) {
    
      assign(file_name, read.csv(path_name, stringsAsFactors=F))
    
  }
  
}





#########################################################
###                    Data cleaning                  ###
#########################################################

##### Overseas territories #####
Mayotte_cases <- file_territory$case[file_territory$place=='Mayotte']
Mayotte_deaths <- file_territory$death[file_territory$place=='Mayotte'] 
Reunion_cases <- file_territory$case[file_territory$place=='R?union']
Reunion_deaths <- file_territory$death[file_territory$place=='R?union']


##### AFRO #####
data_afro <- 
  file_afro %>% 
  filter(!country_name %in% c("Mayotte", "R?union"))

names(data_afro)

data_afro <- data_afro[is.na(data_afro$country_name) == F,]

data_afro <- data_afro[, c("region","country_name","cum_cases","cum_death")]
colnames(data_afro) <- c("report_region", "report_country", "total_cases", "total_deaths")

data_afro <- rbind(data_afro,
                   data.frame(report_region = c("AFRO", "AFRO"),
                              report_country = c("Mayotte", "R?union"),
                              total_cases = c(Mayotte_cases, Reunion_cases),
                              total_deaths = c(Mayotte_deaths, Reunion_deaths)))

data_afro <- rbind(data_afro, data.frame(report_region = "AFRO_total",
                                         report_country = "AFRO_total",
                                         total_cases = sum(data_afro$total_cases[data_afro$report_region == "AFRO"], na.rm = T),
                                         total_deaths = sum(data_afro$total_deaths[data_afro$report_region == "AFRO"], na.rm = T)))
#####

##### PAHO #####
data_paho <- file_paho
names(data_paho)

data_paho <- data_paho[, c("region","country_name","cum_cases","cum_death")]
colnames(data_paho) <- c("report_region", "report_country", "total_cases", "total_deaths")
data_paho <- rbind(data_paho, data.frame(report_region = "PAHO_total",
                                         report_country = "PAHO_total",
                                         total_cases = sum(data_paho$total_cases[data_paho$report_region == "PAHO"], na.rm = T),
                                         total_deaths = sum(data_paho$total_deaths[data_paho$report_region == "PAHO"], na.rm = T)))
#####

##### EMRO #####
data_emro <- file_emro
data_emro <- data_emro[, c("region","country_name","cum_cases","cum_death")]
colnames(data_emro) <- c("report_region", "report_country", "total_cases", "total_deaths")
data_emro <- rbind(data_emro, data.frame(report_region = "EMRO_total",
                                         report_country = "EMRO_total",
                                         total_cases = sum(data_emro$total_cases[data_emro$report_region == "EMRO"], na.rm = T),
                                         total_deaths = sum(data_emro$total_deaths[data_emro$report_region == "EMRO"], na.rm = T)))


##### EURO #####
data_euro <- file_euro
data_euro <- data_euro[, c("region","country_name","cum_cases","cum_death")]
colnames(data_euro) <- c("report_region", "report_country", "total_cases", "total_deaths")

data_euro$report_country[grep('Kosovo', data_euro$report_country)] <- "Kosovo[1]"


### Add 4 cases for UK
data_euro$total_cases[data_euro$report_country == "The United Kingdom"] <- data_euro$total_cases[data_euro$report_country == "The United Kingdom"] + 4

### Remove cases and death from OSTS of France
data_euro$total_cases[data_euro$report_country == "France"] <- data_euro$total_cases[data_euro$report_country == "France"] - 
  sum(data_afro$total_cases[data_afro$report_country %in% c("Mayotte", "R?union")], na.rm = T) - 
  sum(data_paho$total_cases[data_paho$report_country %in% c("Martinique", "Guadeloupe", "French Guiana", "Saint Martin", "Saint Barth?lemy", "Saint Pierre and Miquelon")], na.rm = T)

data_euro$total_deaths[data_euro$report_country == "France"] <- data_euro$total_deaths[data_euro$report_country == "France"] - 
  sum(data_afro$total_deaths[data_afro$report_country %in% c("Mayotte", "R?union")], na.rm = T) - 
  sum(data_paho$total_deaths[data_paho$report_country %in% c("Martinique", "Guadeloupe", "French Guiana", "Saint Martin", "Saint Barth?lemy", "Saint Pierre and Miquelon")], na.rm = T)


data_euro <- rbind(data_euro, data.frame(report_region = "EURO_total",
                                         report_country = "EURO_total",
                                         total_cases = sum(data_euro$total_cases[data_euro$report_region == "EURO"], na.rm = T),
                                         total_deaths = sum(data_euro$total_deaths[data_euro$report_region == "EURO"], na.rm = T)))
#####

##### SEARO #####
data_searo <- file_searo
data_searo <- data_searo[data_searo$region == "SEARO", c("region","country_name","cum_cases","cum_death")]
colnames(data_searo) <- c("report_region", "report_country", "total_cases", "total_deaths")
data_searo <- rbind(data_searo, data.frame(report_region = "SEARO_total",
                                           report_country = "SEARO_total",
                                           total_cases = sum(data_searo$total_cases[data_searo$report_region == "SEARO"], na.rm = T),
                                           total_deaths = sum(data_searo$total_deaths[data_searo$report_region == "SEARO"], na.rm = T)))
#####

##### WPRO #####
data_wpro <- file_wpro
data_wpro <- data_wpro[data_wpro$region == "WPRO", c("region","country_name","cum_cases","cum_death")]

colnames(data_wpro) <-  c("report_region", "report_country", "total_cases", "total_deaths")
data_wpro <- rbind(data_wpro, data.frame(report_region = "WPRO_total",
                                         report_country = "WPRO_total",
                                         total_cases = sum(data_wpro$total_cases[data_wpro$report_region == "WPRO"], na.rm = T),
                                         total_deaths = sum(data_wpro$total_deaths[data_wpro$report_region == "WPRO"], na.rm = T)))
#####


##### Merging all International conveyance together (OTHER) #####
data_other <- file_other
data_other <- data_other[, c("region","country_name","cum_cases","cum_death")]

colnames(data_other) <-  c("report_region", "report_country", "total_cases", "total_deaths")


##### Yesterday Sitrep data #####
sitrep_yesterday <- file_sitrep_yesterday
sitrep_yesterday <- sitrep_yesterday[,c("report_country","report_region","total_cases","new_cases","total_deaths","new_deaths")]
colnames(sitrep_yesterday) <- c("report_country", "report_region", "total_cases_sitrep_yesterday", "new_cases_sitrep_yesterday", 
                                "total_deaths_sitrep_yesterday", "new_deaths_sitrep_yesterday")
#####

##### Today Sitrep data #####
sitrep_today <- file_sitrep_today
colnames(sitrep_today) <- c("report_country", "report_region", "total_cases_sitrep_today", "new_cases_sitrep_today", 
                            "total_deaths_sitrep_today", "new_deaths_sitrep_today","transmission_classification","days_since_last_report")
#####



#########################################################
###                   Merge all data                  ###
#########################################################
##### Combine all regions in one file #####
#data <- rbind(data_afro, data_paho, data_emro, data_euro, data_searo, data_wpro, data_other)
data <- rbind(data_paho, data_wpro, data_afro, data_emro, data_euro, data_searo, data_other)
##### 

##### Calculate Subtotal and Grand total#####
data <- rbind(data, data.frame(report_region = "Subtotal for all Regions",
                               report_country = "Subtotal for all Regions",
                               total_cases = sum(data$total_cases[data$report_region %in% c("AFRO_total","PAHO_total", "EMRO_total", "EURO_total", "SEARO_total", "WPRO_total")]),
                               total_deaths = sum(data$total_deaths[data$report_region %in% c("AFRO_total","PAHO_total", "EMRO_total", "EURO_total", "SEARO_total", "WPRO_total")])))
data <- rbind(data, data.frame(report_region = "Grand total",
                               report_country = "Grand total",
                               total_cases = sum(data$total_cases[data$report_region %in% c("AFRO_total","PAHO_total", "EMRO_total", "EURO_total", "SEARO_total", "WPRO_total", "OTHER")]),
                               total_deaths = sum(data$total_deaths[data$report_region %in% c("AFRO_total","PAHO_total", "EMRO_total", "EURO_total", "SEARO_total", "WPRO_total", "OTHER")])))

##### 

data <- rbind(data[data$report_region != "OTHER",],
              data.frame(report_country = "Other*",
                         report_region = "OTHER",
                         total_cases = sum(data$total_cases[data$report_region == "OTHER"]),
                         total_deaths = sum(data$total_deaths[data$report_region == "OTHER"])))



#####


##### Merge data with yesterday and today sitrep table #####
data <- merge(data, sitrep_yesterday,by = c("report_country", "report_region"), all = T)
data <- merge(data, sitrep_today,by = c("report_country", "report_region"), all = T)
##### 

##### Calculate new cases and deaths #####
data$total_cases <- as.numeric(data$total_cases)
data$total_deaths <- as.numeric(data$total_deaths)

data$total_cases_sitrep_yesterday <- as.numeric(data$total_cases_sitrep_yesterday)
data$new_cases_sitrep_yesterday <- as.numeric(data$new_cases_sitrep_yesterday)
data$total_deaths_sitrep_yesterday <- as.numeric(data$total_deaths_sitrep_yesterday)
data$new_deaths_sitrep_yesterday <- as.numeric(data$new_deaths_sitrep_yesterday)

data$total_cases_sitrep_today <- as.numeric(data$total_cases_sitrep_today)
data$new_cases_sitrep_today <- as.numeric(data$new_cases_sitrep_today)
data$total_deaths_sitrep_today <- as.numeric(data$total_deaths_sitrep_today)
data$new_deaths_sitrep_today <- as.numeric(data$new_deaths_sitrep_today)


#data$new_cases <- data$total_cases - data$total_cases_sitrep_yesterday
#data$new_deaths <- data$total_deaths - data$total_deaths_sitrep_yesterday
data$sitrep_case_differences <- data$total_cases_sitrep_today - data$total_cases_sitrep_yesterday
data$sitrep_death_difference <- data$total_deaths_sitrep_today - data$total_deaths_sitrep_yesterday

#data$new_case_check <- ifelse(data$new_cases == data$sitrep_cases, "Same", "Different")
#data$new_death_check <-ifelse(data$new_deaths == data$sitrep_deaths, "Same", "Different")



##### 


##### Create all the validation checks variables #####
data$case_check_validation <- ifelse(data$total_cases == data$total_cases_sitrep_today, "Same", "Different")
data$death_check_validation <-ifelse(data$total_deaths == data$total_deaths_sitrep_today, "Same", "Different")
#data$new_case_check <- ifelse(data$sitrep_case == data$new_cases_sitrep_today, "Same", "Different")
#data$new_death_check <-ifelse(data$sitrep_death == data$new_deaths_sitrep_today, "Same", "Different")

data$change_total_case_perc <- ifelse(is.na(data$total_cases_sitrep_today) | is.na(data$total_cases_sitrep_yesterday) | data$total_cases_sitrep_yesterday == 0,NA,
                                 (data$total_cases_sitrep_today - data$total_cases_sitrep_yesterday) / data$total_cases_sitrep_yesterday * 100
                                 )
data$change_total_death_perc <- ifelse(is.na(data$total_deaths_sitrep_today) | is.na(data$total_deaths_sitrep_yesterday) | data$total_deaths_sitrep_yesterday == 0,NA,
                                  (data$total_deaths_sitrep_today - data$total_deaths_sitrep_yesterday) / data$total_deaths_sitrep_yesterday * 100
                                  )
#data$change_new_case <- (data$new_cases_sitrep_today - data$new_cases_sitrep_yesterday) / data$new_cases_sitrep_yesterday * 100
#data$change_new_death <- (data$new_deaths_sitrep_today - data$new_deaths_sitrep_yesterday) / data$new_deaths_sitrep_yesterday * 100

#####

##### Final data #####
#data_validation_today <- data[,c("report_country","report_region","total_cases_sitrep_today","new_cases_sitrep_today","total_deaths_sitrep_today",
#                                 "new_deaths_sitrep_today","total_cases","sitrep_case","total_deaths","sitrep_death",
#                                 "transmission_classification","days_since_last_report","total_case_check","total_death_check","new_case_check","new_death_check")]
#
#data_validation_yesterday <- data[,c("report_country","report_region","total_cases_sitrep_yesterday","new_cases_sitrep_yesterday","total_deaths_sitrep_yesterday",
#                                     "new_deaths_sitrep_yesterday","total_cases","total_deaths","change_total_case","change_total_death","change_new_case","change_new_death")]
#
#

data <- data[,c("report_country","report_region",
                "total_cases_sitrep_today","new_cases_sitrep_today","total_deaths_sitrep_today","new_deaths_sitrep_today",
                "total_cases_sitrep_yesterday","new_cases_sitrep_yesterday","total_deaths_sitrep_yesterday","new_deaths_sitrep_yesterday",
                "total_cases","total_deaths",
                "transmission_classification","days_since_last_report",
                "sitrep_case_differences","sitrep_death_difference",
                "case_check_validation","death_check_validation",
                "change_total_case_perc","change_total_death_perc")]
colnames(data) <- c("report_country","report_region",
                    "total_cases_sitrep_today","new_cases_sitrep_today","total_deaths_sitrep_today","new_deaths_sitrep_today",
                    "total_cases_sitrep_yesterday","new_cases_sitrep_yesterday","total_deaths_sitrep_yesterday","new_deaths_sitrep_yesterday",
                    "Regional_case_total","Regional_death_total",
                    "transmission_classification","days_since_last_report",
                    "sitrep_case_differences","sitrep_death_difference",
                    "case_check_validation","death_check_validation",
                    "change_total_case_perc","change_total_death_perc")
#####


##### Save data #####

Update_date <- format(Sys.time(), tz="Europe/Paris", usetz=F)
#Update_date <- format(Sys.time(), tz="America/New_York", usetz=F)


#last_update <- data.frame(Update = as.character(as.POSIXct(Sys.time())))
last_update <- data.frame(Update = as.character(as.POSIXct(Update_date)))




#data <- data[,c("report_country","report_region","total_cases","new_cases","total_deaths","new_deaths","transmission_classification","days_since_last_report"),]

#write.csv(data_validation_today, paste("data_validation_sitrep_today_vs_original_data_", format(Sys.time(), "%Y-%m-%d_%H%M"),".csv", sep = ""))
#write.csv(data_validation_today, paste("data_validation_sitrep_today_vs_original_data",".csv", sep = ""))
#write.csv(data_validation_yesterday, paste("data_validation_sitrep_today_vs_sitrep_yesterday_", format(Sys.time(), "%Y-%m-%d_%H%M"),".csv", sep = ""))
#write.csv(data_validation_yesterday, paste("data_validation_sitrep_today_vs_sitrep_yesterday",".csv", sep = ""))


#write.xlsx(x = data, file = paste("outputs/data_validation_table_", format(as.POSIXct(Update_date), "%Y-%m-%d_%H%M"),".xlsx", sep = ""), 
#           sheetName = "Data", 
#           col.names = TRUE, row.names = F, append = FALSE, showNA=FALSE)
#write.xlsx(x = last_update, file = paste("outputs/data_validation_table_", format(as.POSIXct(Update_date), "%Y-%m-%d_%H%M"),".xlsx", sep = ""), 
#           sheetName = "Last update", 
#           col.names = TRUE, row.names = F, append = T, showNA=FALSE)
#
#write.xlsx(x = data, file = paste("outputs/data_validation_table",".xlsx", sep = ""), 
#           sheetName = "Data", 
#           col.names = TRUE, row.names = F, append = FALSE, showNA=FALSE)
#write.xlsx(x = last_update, file = paste("outputs/data_validation_table",".xlsx", sep = ""), 
#           sheetName = "Last update", 
#           col.names = TRUE, row.names = F, append = T, showNA=FALSE)


#ExcelFile1 <- loadWorkbook(filename = paste("outputs/data_validation_table_", format(as.POSIXct(Update_date), "%Y-%m-%d_%H%M"),".xlsx", sep = ""), create = TRUE )
#createSheet(object = ExcelFile1, name = "Data")
#writeWorksheet(object = ExcelFile1, data = data, sheet = "Data", startRow = 1, startCol = 1, header = TRUE )
#createSheet(object = ExcelFile1, name = "Last update")
#writeWorksheet(object = ExcelFile1, data = last_update, sheet = "Last update", startRow = 1, startCol = 1, header = TRUE )
#saveWorkbook(ExcelFile1)
#
#
#
#ExcelFile2 <- loadWorkbook(filename = paste("outputs/data_validation_table",".xlsx", sep = ""), create = TRUE )
#createSheet(object = ExcelFile2, name = "Data")
#writeWorksheet(object = ExcelFile2, data = data, sheet = "Data", startRow = 1, startCol = 1, header = TRUE )
#createSheet(object = ExcelFile2, name = "Last update")
#writeWorksheet(object = ExcelFile2, data = last_update, sheet = "Last update", startRow = 1, startCol = 1, header = TRUE )
#saveWorkbook(ExcelFile2)

data <- mutate(data,
               percent_change_new_cases = (new_cases_sitrep_today - new_cases_sitrep_yesterday) / new_cases_sitrep_yesterday * 100,
               percent_change_new_deaths = (new_deaths_sitrep_today - new_deaths_sitrep_yesterday) / new_deaths_sitrep_yesterday * 100) %>%
  mutate_at(vars(starts_with("percent")), ~ifelse(is.infinite(.), NA, .))

# -- section for getting weekly data 

require(tidyverse)
require(httr)
require(jsonlite)
require(lubridate)



resp_phi <- GET("https://extranet.who.int/xmart-api/odata/NCOV/V_FACT_DAY_SITREP")
json_phi <- content(resp_phi, "text")
phi_xmart <- as_tibble(fromJSON(json_phi)$value)
# remove FK from xmart headers
colnames(phi_xmart) <- gsub("_FK$", "", colnames(phi_xmart))
# match header cases
colnames(phi_xmart) <- stringr::str_to_lower(colnames(phi_xmart))
message("read phi data from xmart")


phi_xmart <- phi_xmart %>%
  filter(for_public == TRUE) %>%
  mutate(report_country = coalesce(place_exception, whe_name, adm0_title),
         report_date = ymd_hms(report_date)) %>%
  select(report_country,
         iso3 = iso_3_code,
         who_region,
         report_date,
         case_total = cum_cases_confirmed,
         case_new = new_cases_confirmed,
         death_total = cum_cases_deaths,
         death_new = new_cases_deaths) %>%
  mutate(report_country = ifelse(report_country == "DIAMOND",
                                 "International conveyance (Diamond Princess)",
                                 report_country)) %>%
  mutate(report_country = ifelse(grepl("Kosovo", report_country),
                                 "Kosovo[1]", report_country)) %>%
  mutate(report_country = ifelse(grepl("C?te d'Ivoire", report_country),
                                 "C?te d'Ivoire", report_country)) %>%
  mutate(who_region = ifelse(is.na(who_region), "OTHER", who_region)) %>%
  mutate(who_region = ifelse(who_region == "AMRO", "PAHO", who_region))

current_week = isoweek(Sys.Date())

week_data <- phi_xmart %>%
  mutate(week = isoweek(report_date)) %>%
  filter(week < current_week, week >= (current_week - 5)) %>%
  group_by(report_country, iso3, who_region, week) %>%
  summarise(case_new = max(case_total, na.rm = T),
            death_new = max(death_total, na.rm = T)) %>%
  ungroup()

week_data <- week_data %>%
  mutate(rev_week = paste0("neg_week_", current_week - week)) %>%
  select(-week)

week_data <- week_data %>%
  nest(data = c(case_new, death_new)) %>%
  spread(key = rev_week, value = data) %>%
  unnest(c(neg_week_1, neg_week_2, neg_week_3, neg_week_4, neg_week_5), names_repair = "universal") %>%
  select(report_country,
         who_region,
         cases_1_week_ago = case_new...4,
         cases_2_week_ago = case_new...6,
         cases_3_week_ago = case_new...8,
         cases_4_week_ago = case_new...10,
         cases_5_week_ago = case_new...12,
         deaths_1_week_ago = death_new...5,
         deaths_2_week_ago = death_new...7,
         deaths_3_week_ago = death_new...9,
         deaths_4_week_ago = death_new...11,
         deaths_5_week_ago = death_new...13
  ) %>%
  mutate(perc_change_cases_1_week_ago = (cases_1_week_ago - cases_2_week_ago) / cases_2_week_ago * 100,
         perc_change_cases_2_week_ago = (cases_2_week_ago - cases_3_week_ago) / cases_3_week_ago * 100,
         perc_change_cases_3_week_ago = (cases_3_week_ago - cases_4_week_ago) / cases_4_week_ago * 100,
         perc_change_cases_4_week_ago = (cases_4_week_ago - cases_5_week_ago) / cases_5_week_ago * 100,
         perc_change_deaths_1_week_ago = (deaths_1_week_ago - deaths_2_week_ago) / deaths_2_week_ago * 100,
         perc_change_deaths_2_week_ago = (deaths_2_week_ago - deaths_3_week_ago) / deaths_3_week_ago * 100,
         perc_change_deaths_3_week_ago = (deaths_3_week_ago - deaths_4_week_ago) / deaths_4_week_ago * 100,
         perc_change_deaths_4_week_ago = (deaths_4_week_ago - deaths_5_week_ago) / deaths_5_week_ago * 100)


week_regions = week_data %>%
  select(-report_country) %>%
  group_by(who_region) %>%
  summarise_all(~sum(., na.rm = T)) %>%
  mutate(perc_change_cases_1_week_ago = (cases_1_week_ago - cases_2_week_ago) / cases_2_week_ago * 100,
         perc_change_cases_2_week_ago = (cases_2_week_ago - cases_3_week_ago) / cases_3_week_ago * 100,
         perc_change_cases_3_week_ago = (cases_3_week_ago - cases_4_week_ago) / cases_4_week_ago * 100,
         perc_change_cases_4_week_ago = (cases_4_week_ago - cases_5_week_ago) / cases_5_week_ago * 100,
         perc_change_deaths_1_week_ago = (deaths_1_week_ago - deaths_2_week_ago) / deaths_2_week_ago * 100,
         perc_change_deaths_2_week_ago = (deaths_2_week_ago - deaths_3_week_ago) / deaths_3_week_ago * 100,
         perc_change_deaths_3_week_ago = (deaths_3_week_ago - deaths_4_week_ago) / deaths_4_week_ago * 100,
         perc_change_deaths_4_week_ago = (deaths_4_week_ago - deaths_5_week_ago) / deaths_5_week_ago * 100) %>%
  mutate(report_country = paste0(who_region, "_total")) %>%
  ungroup()

week_totals <- week_data %>%
  mutate(region2 = ifelse(who_region == "OTHER", "OTHER", "region")) %>%
  select(-report_country, -who_region) %>%
  group_by(region2) %>%
  summarise_all(~sum(., na.rm = T)) %>%
  mutate(perc_change_cases_1_week_ago = (cases_1_week_ago - cases_2_week_ago) / cases_2_week_ago * 100,
         perc_change_cases_2_week_ago = (cases_2_week_ago - cases_3_week_ago) / cases_3_week_ago * 100,
         perc_change_cases_3_week_ago = (cases_3_week_ago - cases_4_week_ago) / cases_4_week_ago * 100,
         perc_change_cases_4_week_ago = (cases_4_week_ago - cases_5_week_ago) / cases_5_week_ago * 100,
         perc_change_deaths_1_week_ago = (deaths_1_week_ago - deaths_2_week_ago) / deaths_2_week_ago * 100,
         perc_change_deaths_2_week_ago = (deaths_2_week_ago - deaths_3_week_ago) / deaths_3_week_ago * 100,
         perc_change_deaths_3_week_ago = (deaths_3_week_ago - deaths_4_week_ago) / deaths_4_week_ago * 100,
         perc_change_deaths_4_week_ago = (deaths_4_week_ago - deaths_5_week_ago) / deaths_5_week_ago * 100) %>%
  ungroup()

week_subt <- week_totals %>%
  mutate(report_country = ifelse(region2 == "OTHER", "Other*", "Subtotal for all Regions")) %>%
  select(-region2)

week_total = week_subt %>%
  select(-report_country) %>%
  summarise_all(~sum(., na.rm = T)) %>%
  mutate(perc_change_cases_1_week_ago = (cases_1_week_ago - cases_2_week_ago) / cases_2_week_ago * 100,
         perc_change_cases_2_week_ago = (cases_2_week_ago - cases_3_week_ago) / cases_3_week_ago * 100,
         perc_change_cases_3_week_ago = (cases_3_week_ago - cases_4_week_ago) / cases_4_week_ago * 100,
         perc_change_cases_4_week_ago = (cases_4_week_ago - cases_5_week_ago) / cases_5_week_ago * 100,
         perc_change_deaths_1_week_ago = (deaths_1_week_ago - deaths_2_week_ago) / deaths_2_week_ago * 100,
         perc_change_deaths_2_week_ago = (deaths_2_week_ago - deaths_3_week_ago) / deaths_3_week_ago * 100,
         perc_change_deaths_3_week_ago = (deaths_3_week_ago - deaths_4_week_ago) / deaths_4_week_ago * 100,
         perc_change_deaths_4_week_ago = (deaths_4_week_ago - deaths_5_week_ago) / deaths_5_week_ago * 100) %>%
  mutate(report_country = "Grand total")




week_all <- bind_rows(week_data, week_regions) %>%
  bind_rows(., week_subt) %>%
  bind_rows(., week_total)

week_all <- week_all %>%
  mutate_at(vars(contains("neg")), ~ifelse(is.infinite(.), NA, .)) %>%
  select(-who_region)


data <- left_join(data, week_all)
data <- data %>%
  mutate_all( ~ifelse(is.infinite(.), NA, .))

data %>% filter(report_country !="") -> data

sheets <- list("Data" = data, "Last update" = last_update) #assume sheet1 and sheet2 are data frames
write_xlsx(sheets, paste("outputs/data_validation_table",".xlsx", sep = ""))
#write_xlsx(sheets, paste("draft/outputs/data_validation_table",".xlsx", sep = ""))
write_xlsx(sheets, paste("outputs/data_validation_table_", format(as.POSIXct(Update_date), "%Y-%m-%d_%H%M"),".xlsx", sep = ""))


##### 




cat("\n\n\nThis is complete. Please open Power BI and proceed with validation.\n\n\n ")
