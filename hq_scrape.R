# --- hq_scrape.R ---
# This script fetches both outages and polygons every hour

library(httr)
library(jsonlite)
library(dplyr)
library(tidyr)
library(stringr)
library(lubridate)
library(arrow)
library(sf)  # needed for polygons
library(readr)   # add this

TZ <- "America/Toronto"

source("hq_functions.R")  # your big script with get_hq_outages etc.

# --- Create folders ---
ts <- with_tz(Sys.time(), TZ)
date_str <- format(ts, "%Y-%m-%d")
hour_str <- format(ts, "%H")
dir_data <- file.path("data", paste0("date=", date_str), paste0("hour=", hour_str))
dir.create(dir_data, recursive = TRUE, showWarnings = FALSE)

# --- 1) Get outages ---
outages <- get_hq_outages(FALSE)
out_path <- file.path(dir_data, paste0("outages_", format(ts, "%Y%m%dT%H%M%S"), ".parquet"))
arrow::write_parquet(outages, out_path)
cat("Saved outages to:", out_path, "\n")

# --- 2) Get polygons ---
polys <- get_hq_polygons(add_metadata = TRUE)
poly_path <- file.path(dir_data, paste0("polygons_", format(ts, "%Y%m%dT%H%M%S"), ".geojson"))
sf::st_write(polys, poly_path, delete_dsn = TRUE, quiet = TRUE)
cat("Saved polygons to:", poly_path, "\n")

# --- 3) (Optional) Join outages to polygons ---
joined <- hq_outages_join_polygons(polys, get_hq_outages(TRUE))
joined_summary <- joined$summary
joined_path <- file.path(dir_data, paste0("outages_joined_", format(ts, "%Y%m%dT%H%M%S"), ".csv"))
readr::write_csv(joined_summary, joined_path)
cat("Saved joined summary to:", joined_path, "\n")
