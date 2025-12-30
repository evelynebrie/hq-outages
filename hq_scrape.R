# --- hq_scrape.R (ROBUST VERSION) ---
# This script fetches both outages and polygons every hour with better error handling

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
source("hq_functions.R")

# --- Create folders ---
ts <- with_tz(Sys.time(), TZ)
date_str <- format(ts, "%Y-%m-%d")
hour_str <- format(ts, "%H")
dir_data <- file.path("data", paste0("date=", date_str), paste0("hour=", hour_str))
dir.create(dir_data, recursive = TRUE, showWarnings = FALSE)

existing_files <- list.files(dir_data, pattern = "\\.(parquet|geojson|csv)$")
if (length(existing_files) > 0) {
  cat("Data already exists for", format(ts, "%Y-%m-%d %H:00"), "- skipping\n")
  quit(save = "no", status = 0)
}

# --- 1) Get outages with error handling ---
cat("Fetching outages...\n")
outages_result <- tryCatch({
  outages <- get_hq_outages(FALSE)
  if (nrow(outages) == 0) {
    cat("Warning: No outages found (this is normal if there are no current outages)\n")
  } else {
    cat("Successfully fetched", nrow(outages), "outages\n")
  }
  out_path <- file.path(dir_data, paste0("outages_", format(ts, "%Y%m%dT%H%M%S"), ".parquet"))
  arrow::write_parquet(outages, out_path)
  cat("✓ Saved outages to:", out_path, "\n")
  list(success = TRUE, data = outages)
}, error = function(e) {
  cat("✗ Failed to fetch outages:", conditionMessage(e), "\n")
  list(success = FALSE, error = conditionMessage(e))
})

# --- 2) Get polygons with error handling ---
cat("\nFetching polygons...\n")
polys_result <- tryCatch({
  polys <- get_hq_polygons(add_metadata = TRUE)
  if (nrow(polys) == 0) {
    cat("Warning: No polygons found\n")
  } else {
    cat("Successfully fetched", nrow(polys), "polygons\n")
  }
  poly_path <- file.path(dir_data, paste0("polygons_", format(ts, "%Y%m%dT%H%M%S"), ".geojson"))
  sf::st_write(polys, poly_path, delete_dsn = TRUE, quiet = TRUE)
  cat("✓ Saved polygons to:", poly_path, "\n")
  list(success = TRUE, data = polys)
}, error = function(e) {
  cat("✗ Failed to fetch polygons:", conditionMessage(e), "\n")
  list(success = FALSE, error = conditionMessage(e))
})

# --- 3) Join (only if both succeeded) ---
if (outages_result$success && polys_result$success && 
    nrow(outages_result$data) > 0 && nrow(polys_result$data) > 0) {
  
  cat("\nCreating spatial join...\n")
  join_result <- tryCatch({
    joined <- hq_outages_join_polygons(
      polys_result$data, 
      get_hq_outages(TRUE)  # Need sf version for join
    )
    joined_summary <- joined$summary
    joined_path <- file.path(dir_data, paste0("outages_joined_", format(ts, "%Y%m%dT%H%M%S"), ".csv"))
    readr::write_csv(joined_summary, joined_path)
    cat("✓ Saved joined summary to:", joined_path, "\n")
    TRUE
  }, error = function(e) {
    cat("✗ Failed to create join:", conditionMessage(e), "\n")
    FALSE
  })
} else {
  cat("\nSkipping join (missing outages or polygons data)\n")
}

# --- Summary ---
cat("\n=== SCRAPE SUMMARY ===\n")
cat("Timestamp:", format(ts, "%Y-%m-%d %H:%M:%S %Z"), "\n")
cat("Outages: ", if(outages_result$success) "✓ Success" else "✗ Failed", "\n")
cat("Polygons:", if(polys_result$success) "✓ Success" else "✗ Failed", "\n")

# Exit with appropriate status
# Success if at least polygons worked (they're the critical data)
exit_status <- if(polys_result$success) 0 else 1
quit(save = "no", status = exit_status)
