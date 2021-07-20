# Connect to the database
con <- odbcDriverConnect("Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=./db_vneshtorg.accdb")

# Load auxialiary files
country_list <- "auxiliary_files/countries.xlsx" %>% 
  read_excel() %>% 
  select(country_oldname, country_code)
region_list <- "auxiliary_files/regions.xlsx" %>% 
  read_excel() %>% 
  select(region_oldname, region_code)
operation_list <- "auxiliary_files/operations.xlsx" %>% 
  read_excel() %>% 
  select(operation_oldname, operation_code)

# Load and tidy new data
df <- "input/data.xlsx" %>% 
  read_excel() %>% 
  select(napr, period, strana, tnved, Stoim, Region) %>% 
  rename(operation_oldname = napr,
         region_oldname = Region,
         date = period,
         country_oldname = strana,
         tnved_item_code = tnved,
         USD = Stoim) %>% 
  mutate(USD = str_replace(USD, ",", ".")) %>% 
  right_join(region_list, by = "region_oldname") %>% 
  select(operation_oldname, date, tnved_item_code, USD, region_code, country_oldname) %>% 
  right_join(country_list, by = "country_oldname") %>% 
  select(date, tnved_item_code, USD, region_code, country_code, operation_oldname) %>% 
  right_join(operation_list, by = "operation_oldname") %>% 
  select(operation_code, date, tnved_item_code, USD, region_code, country_code) %>% 
  drop_na() %>% 
  mutate(date = as_date(str_c(str_extract(date, "[0-9]{4}"), str_extract(date, "[0-9]{2}"), "01", sep = "-")),
         USD = as.numeric(USD))

# Set up a threshold date
threshold <- min(df$date)
threshold <- paste("#", month(threshold), "/", day(threshold), "/", year(threshold), "#", sep = "")

# Delete old data
con %>%
  sqlQuery(paste("DELETE FROM data WHERE (data.date) >=", threshold))

# Load new data at the database 
con %>% 
  sqlSave(dat = df, tablename = "data", rownames = F, append = T, safer = T)

# Close connection to the database
close(con)
