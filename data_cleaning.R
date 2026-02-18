library(glue)
library(readr)
library(tools)
library(duckdb)
library(duckplyr)
library(lubridate)

# Inspecting small sample -------------------------------------------------


path <- "~/R PRACTICE/FL_EMSTARS_csv"

files <- list.files(
  path,
  pattern = "\\.csv$",
  full.names = T
)

system.time(
  data <- lapply(files, read_csv, n_max = 1000)
)

names(data) <- file_path_sans_ext(basename(files))


# DuckDB ------------------------------------------------------------------


# Start Connection
con <- dbConnect(duckdb(), dbdir = "~/R PRACTICE/research_db.duckdb", read_only = FALSE)

# View Tables
dbListTables(con)
  # Drop [SPECIFIC] Table
  # dbExecute(con, "DROP TABLE [TABLE NAME];")

# Drop ALL tables
tables <- dbListTables(con)
for (table in tables) {
  dbExecute(con, paste0("DROP TABLE ", table, ";"))
}


# Disconnect
dbDisconnect(con, shutdown = TRUE)


# Retrieving Data ---------------------------------------------------------


for (i in seq_along(files)) {
  
  table_name <- file_path_sans_ext(basename(files[i]))
  
  query <- glue("
    CREATE TABLE {table_name} AS
    SELECT *
    FROM read_csv_auto('{files[i]}', nullstr = '.', all_varchar=true, ignore_errors = TRUE)
  ")
  
  iteration_time <- system.time({
    dbExecute(con, query)
  })
  
  print(paste("Iteration", i, table_name, "complete"))
  print(iteration_time)
}


# Cleaning Data -----------------------------------------------------------


to_NA <- c("7701003", "7701001", "7701005", "Not Recorded", "Not Applicable", "", ".")


tbl(con, "event") %>% 
  glimpse() %>% 
  count()

tbl(con, "event") %>% 
  select() %>% # SELECT VARIABLES OF INTEREST
  mutate(across(everything(), ~ if_else(.x %in% !!to_NA, NA, .x))) %>%  # VERIFY WHAT IS ACTUALLY NA
  mutate(across(contains("DateTime"), ~ strptime(.x, '%d%b%Y:%H:%M:%S.%f'))) %>% # REMOVE MILLISECONDS, PASTE ON TZs, PARSE, THEN STANDARDIZE
  mutate() # I need to change data types where needed and also re-code elements for variables

tbl(con, "personnel") %>% 
  glimpse()

tbl(con, "patient") %>% 
  glimpse()

tbl(con, "agency") %>% 
  glimpse()

