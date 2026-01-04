# --- hq_scrape.R (FIXED for folder structure) ---
# This script fetches both outages and polygons every hour
# and saves them in the partitioned structure: date=YYYY-MM-DD/hour=HH/

library(httr)
library(jsonlite)
library(dplyr)
library(tidyr)
library(stringr)
library(lubridate)
library(arrow)
library(sf)
library(readr)

TZ <- "America/Toronto"

source("hq_functions.R")  # your big script with get_hq_outages etc.

# --- Get current timestamp ---
ts <- with_tz(Sys.time(), TZ)
date_str <- format(ts, "%Y-%m-%d")
hour_str <- format(ts, "%H")

# --- Create partitioned directory structure ---
dir_data <- file.path("data", paste0("date=", date_str), paste0("hour=", hour_str))
dir.create(dir_data, recursive = TRUE, showWarnings = FALSE)

# --- Check if data already exists for this hour ---
existing_files <- list.files(dir_data, pattern = "\\.(parquet|geojson|csv)$")
if (length(existing_files) > 0) {
  cat("Data already exists for", format(ts, "%Y-%m-%d %H:00"), "- skipping\n")
  cat("  Files found:", paste(existing_files, collapse = ", "), "\n")
  quit(save = "no", status = 0)
}

cat("Processing data for", format(ts, "%Y-%m-%d %H:%M:%S"), "\n")
cat("Output directory:", dir_data, "\n")

# --- 1) Get outages (optional - may fail if no data available) ---
cat("\n[1/3] Fetching outage data...\n")
outages_success <- FALSE
tryCatch({
  outages <- get_hq_outages(FALSE)
  if (nrow(outages) > 0) {
    out_path <- file.path(dir_data, paste0("outages_", format(ts, "%Y%m%dT%H%M%S"), ".parquet"))
    arrow::write_parquet(outages, out_path)
    cat("  ✓ Saved", nrow(outages), "outages to:", out_path, "\n")
    outages_success <- TRUE
  } else {
    cat("  ⚠ No outages found in API response\n")
  }
}, error = function(e) {
  cat("  ⚠ Failed to fetch outages:", e$message, "\n")
  cat("  Continuing without outage data...\n")
})

# --- 2) Get polygons (REQUIRED) ---
cat("\n[2/3] Fetching polygon data...\n")
polygons_success <- FALSE
tryCatch({
  polys <- get_hq_polygons(add_metadata = TRUE)
  
  if (nrow(polys) > 0) {
    poly_path <- file.path(dir_data, paste0("polygons_", format(ts, "%Y%m%dT%H%M%S"), ".geojson"))
    sf::st_write(polys, poly_path, delete_dsn = TRUE, quiet = TRUE)
    cat("  ✓ Saved", nrow(polys), "polygons to:", poly_path, "\n")
    polygons_success <- TRUE
  } else {
    cat("  ⚠ No polygons found in API response\n")
    cat("  This likely means there are no active outages right now\n")
  }
}, error = function(e) {
  cat("  ✗ Failed to fetch polygons:", e$message, "\n")
  stop("Cannot continue without polygon data")
})

# --- 3) Join outages to polygons (OPTIONAL - only if both succeeded) ---
cat("\n[3/3] Creating joined summary...\n")
if (outages_success && polygons_success) {
  tryCatch({
    # Need to get outages as SF for the join
    outages_sf <- get_hq_outages(TRUE)
    
    joined <- hq_outages_join_polygons(polys, outages_sf)
    joined_summary <- joined$summary
    
    if (nrow(joined_summary) > 0) {
      joined_path <- file.path(dir_data, paste0("outages_joined_", format(ts, "%Y%m%dT%H%M%S"), ".csv"))
      readr::write_csv(joined_summary, joined_path)
      cat("  ✓ Saved joined summary to:", joined_path, "\n")
    } else {
      cat("  ⚠ Join produced no results (outages might not overlap with polygons)\n")
    }
  }, error = function(e) {
    cat("  ⚠ Failed to create joined summary:", e$message, "\n")
    cat("  Continuing without joined data...\n")
  })
} else {
  cat("  ⊘ Skipping join (need both outages and polygons)\n")
  if (!outages_success) cat("    - Outages not available\n")
  if (!polygons_success) cat("    - Polygons not available\n")
}

# --- Summary ---
cat("\n========================================\n")
cat("SCRAPE COMPLETE\n")
cat("Timestamp:", format(ts, "%Y-%m-%d %H:%M:%S %Z"), "\n")
cat("Directory:", dir_data, "\n")

final_files <- list.files(dir_data, full.names = FALSE)
cat("Files created:", length(final_files), "\n")
if (length(final_files) > 0) {
  cat("  -", paste(final_files, collapse = "\n  - "), "\n")
}

cat("========================================\n")

# Exit with success if we got at least the polygons
if (polygons_success) {
  quit(save = "no", status = 0)
} else {
  cat("\n⚠ WARNING: No polygon data collected - this might indicate:\n")
  cat("  1. No active outages at this time\n")
  cat("  2. API connectivity issues\n")
  cat("  3. Data format changes\n")
  
  # Don't fail completely - might just be no outages
  quit(save = "no", status = 0)
}
