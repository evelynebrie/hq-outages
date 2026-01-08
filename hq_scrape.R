# --- hq_scrape.R (IMPROVED for large datasets) ---
# This script fetches both outages and polygons every hour with better error handling
# and memory efficiency for large datasets

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

# --- Configuration ---
MAX_RETRIES <- 3
TIMEOUT_SEC <- 60  # Increased from 20 to 60
CHUNK_SIZE <- 1000  # Process outages in chunks for spatial joins

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

# --- 1) Get outages with better error handling ---
cat("\n[1/3] Fetching outage data...\n")
outages_success <- FALSE
outages <- NULL

tryCatch({
  outages <- get_hq_outages(FALSE)
  
  if (nrow(outages) > 0) {
    cat("  → Found", nrow(outages), "outages\n")
    
    # Save in chunks if very large
    out_path <- file.path(dir_data, paste0("outages_", format(ts, "%Y%m%dT%H%M%S"), ".parquet"))
    
    if (nrow(outages) > 10000) {
      cat("  → Large dataset detected, using compression...\n")
      arrow::write_parquet(outages, out_path, compression = "snappy")
    } else {
      arrow::write_parquet(outages, out_path)
    }
    
    cat("  ✓ Saved outages to:", out_path, "\n")
    outages_success <- TRUE
  } else {
    cat("  ⚠ No outages found in API response\n")
  }
}, error = function(e) {
  cat("  ⚠ Failed to fetch outages:", e$message, "\n")
  cat("  Continuing without outage data...\n")
})

# --- 2) Get polygons ---
cat("\n[2/3] Fetching polygon data...\n")
polygons_success <- FALSE
polys <- NULL

tryCatch({
  polys <- get_hq_polygons(add_metadata = TRUE)
  
  if (nrow(polys) > 0) {
    cat("  → Found", nrow(polys), "polygons\n")
    poly_path <- file.path(dir_data, paste0("polygons_", format(ts, "%Y%m%dT%H%M%S"), ".geojson"))
    sf::st_write(polys, poly_path, delete_dsn = TRUE, quiet = TRUE)
    cat("  ✓ Saved polygons to:", poly_path, "\n")
    polygons_success <- TRUE
  } else {
    cat("  ⚠ No polygons found in API response\n")
    cat("  This likely means there are no active outages right now\n")
  }
}, error = function(e) {
  cat("  ✗ Failed to fetch polygons:", e$message, "\n")
  stop("Cannot continue without polygon data")
})

# --- 3) Join outages to polygons with chunking for large datasets ---
cat("\n[3/3] Creating joined summary...\n")

if (outages_success && polygons_success) {
  tryCatch({
    # Get outages as SF
    outages_sf <- get_hq_outages(TRUE)
    n_outages <- nrow(outages_sf)
    
    cat("  → Processing", n_outages, "outage points\n")
    
    # For large datasets, use chunked processing
    if (n_outages > CHUNK_SIZE) {
      cat("  → Large dataset detected, using chunked processing...\n")
      
      # Process in chunks
      n_chunks <- ceiling(n_outages / CHUNK_SIZE)
      joined_list <- vector("list", n_chunks)
      
      for (i in seq_len(n_chunks)) {
        start_idx <- (i - 1) * CHUNK_SIZE + 1
        end_idx <- min(i * CHUNK_SIZE, n_outages)
        
        cat("    Processing chunk", i, "of", n_chunks, 
            "(rows", start_idx, "to", end_idx, ")\n")
        
        chunk_sf <- outages_sf[start_idx:end_idx, ]
        chunk_joined <- st_join(chunk_sf, polys, join = st_within, left = FALSE)
        joined_list[[i]] <- chunk_joined
        
        # Force garbage collection to free memory
        if (i %% 5 == 0) gc()
      }
      
      # Combine chunks
      cat("  → Combining chunks...\n")
      joined_full <- do.call(rbind, joined_list)
      rm(joined_list)
      gc()
      
    } else {
      # Small dataset - process normally
      joined_full <- st_join(outages_sf, polys, join = st_within, left = FALSE)
    }
    
    # Create summary
    cat("  → Creating summary statistics...\n")
    joined_df <- st_drop_geometry(joined_full)
    
    # Determine polygon ID column
    poly_id <- NULL
    for (cand in c("poly_id", "id", "ID", "name", "NOM", "MUNICIPALITE")) {
      if (cand %in% names(joined_df)) {
        poly_id <- cand
        break
      }
    }
    
    if (is.null(poly_id)) {
      cat("  ⚠ No standard polygon ID found, using row numbers\n")
      joined_df$poly_id <- seq_len(nrow(joined_df))
      poly_id <- "poly_id"
    }
    
    # Summarize
    if ("customers" %in% names(joined_df)) {
      joined_summary <- joined_df %>%
        group_by(.data[[poly_id]]) %>%
        summarise(
          n_outages = n(),
          customers_sum = sum(customers, na.rm = TRUE),
          customers_mean = mean(customers, na.rm = TRUE),
          .groups = "drop"
        )
    } else {
      joined_summary <- joined_df %>%
        group_by(.data[[poly_id]]) %>%
        summarise(
          n_outages = n(),
          .groups = "drop"
        )
    }
    
    if (nrow(joined_summary) > 0) {
      joined_path <- file.path(dir_data, paste0("outages_joined_", format(ts, "%Y%m%dT%H%M%S"), ".csv"))
      readr::write_csv(joined_summary, joined_path)
      cat("  ✓ Saved joined summary (", nrow(joined_summary), " polygons) to:", joined_path, "\n")
      
      # Also save the full joined data if it's not too large
      if (nrow(joined_full) <= 50000) {
        joined_full_path <- file.path(dir_data, paste0("outages_joined_full_", format(ts, "%Y%m%dT%H%M%S"), ".geojson"))
        st_write(joined_full, joined_full_path, delete_dsn = TRUE, quiet = TRUE)
        cat("  ✓ Saved full joined data to:", joined_full_path, "\n")
      } else {
        cat("  ⚠ Skipping full joined data (too large:", nrow(joined_full), "rows)\n")
      }
    } else {
      cat("  ⚠ Join produced no results (outages might not overlap with polygons)\n")
    }
    
  }, error = function(e) {
    cat("  ⚠ Failed to create joined summary:", e$message, "\n")
    cat("  Stack trace:\n")
    cat(paste0("    ", capture.output(traceback()), collapse = "\n"), "\n")
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

# Memory usage report
mem_used <- pryr::mem_used()
cat("Memory used:", format(mem_used, units = "auto"), "\n")

cat("========================================\n")

# Exit with success if we got at least the polygons
if (polygons_success) {
  quit(save = "no", status = 0)
} else {
  cat("\n⚠ WARNING: No polygon data collected\n")
  quit(save = "no", status = 0)
}
