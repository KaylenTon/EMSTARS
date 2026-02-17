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


# DuckDB attempt ----------------------------------------------------------


# Start Connection
con <- dbConnect(duckdb(), dbdir = "~/R PRACTICE/research_db.duckdb", read_only = FALSE)

# View Tables
dbListTables(con)
  # Drop Table
  # dbExecute(con, "DROP TABLE [REPLACE];")

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
                   FROM patient1 
                   LIMIT 5;")


# Disconnect --------------------------------------------------------------

dbDisconnect(con, shutdown = TRUE)

# test --------------------------------------------------------------------

