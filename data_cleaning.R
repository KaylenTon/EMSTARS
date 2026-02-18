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
  # Drop Table
  # dbExecute(con, "DROP TABLE *;")

# Drop ALL tables
tables <- dbListTables(con)
for (table in tables) {
  dbExecute(con, paste0("DROP TABLE ", table, ";"))
}


# Disconnect
dbDisconnect(con, shutdown = TRUE)


# Retrieving Data ---------------------------------------------------------


dbExecute(con, "
CREATE TABLE testing AS
SELECT 
  PatientId::VARCHAR AS PatientId,
  ReportsID::VARCHAR AS ReportsID,
  Gender::DOUBLE AS Gender,
  Age::DOUBLE AS Age
FROM read_csv_auto(
  'C:/Users/Kaylen/OneDrive - University of South Florida/Documents/R PRACTICE/FL_EMSTARS_csv/patient.csv',
  nullstr='.'
)
")

test <- dbGetQuery(con, 
                   "SELECT * 
                   FROM crew 
                   LIMIT 5;")


# Disconnect --------------------------------------------------------------

dbDisconnect(con, shutdown = TRUE)

# test --------------------------------------------------------------------

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
