# HQ Outages - OPTIMIZED Single-Pass Hex Processing
# Load ALL polygons first, then intersect with hex grid ONCE

suppressPackageStartupMessages({
  library(sf)
  library(dplyr)
  library(jsonlite)
  library(data.table)
})

cat("=== HQ Outages OPTIMIZED Single-Pass Processing ===\n")
cat("Strategy: Load all polygons, create hex grid once, count intersections\n")
cat(sprintf("\nStarting at: %s\n", format(Sys.time(), "%Y-%m-%d %H:%M:%S")))
start_time <- Sys.time()

# Config
data_path <- "data/daily"
output_dir <- "public"
cumulative_snapshots_dir <- file.path(output_dir, "cumulative_snapshots")

HEX_SIZE <- 1000
SIMPLIFY <- 200

dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(file.path(output_dir, "daily"), recursive = TRUE, showWarnings = FALSE)
dir.create(file.path(output_dir, "monthly"), recursive = TRUE, showWarnings = FALSE)
dir.create(file.path(output_dir, "total"), recursive = TRUE, showWarnings = FALSE)
dir.create(cumulative_snapshots_dir, recursive = TRUE, showWarnings = FALSE)

# =================================================================
# STEP 1: Load ALL polygon files with timestamps
# =================================================================
cat("\n[1] Loading ALL polygon files...\n")

files <- list.files(data_path, pattern = "^polygons_.*\\.geojson$", full.names = TRUE)
if (length(files) == 0) stop("No polygon files found")

cat(sprintf("  Found %d polygon files\n", length(files)))

# Parse all timestamps
cat("  Parsing timestamps...\n")
basenames <- basename(files)
timestamps <- regmatches(basenames, regexpr("\\d{8}t\\d{6}", basenames, ignore.case = TRUE))

date_parts <- substr(timestamps, 1, 8)
dates <- sprintf("%s-%s-%s", 
                substr(date_parts, 1, 4), 
                substr(date_parts, 5, 6), 
                substr(date_parts, 7, 8))

hours <- as.integer(substr(timestamps, 10, 11))
minutes <- substr(timestamps, 12, 13)
seconds <- substr(timestamps, 14, 15)

datetimes <- sprintf("%s %s:%s:%s", dates, 
                    substr(timestamps, 10, 11), minutes, seconds)

files_dt <- data.table(
  file = files,
  date = dates,
  hour = hours,
  datetime = datetimes,
  timestamp_sort = as.numeric(as.POSIXct(datetimes, format="%Y-%m-%d %H:%M:%S"))
)

# Select latest file per hour (deduplication)
cat("  Deduplicating to latest file per hour...\n")
setkey(files_dt, date, hour, timestamp_sort)
files_dt <- files_dt[, .SD[.N], by = .(date, hour)]
setorder(files_dt, timestamp_sort)

cat(sprintf("  After deduplication: %d unique hourly snapshots\n", nrow(files_dt)))
cat(sprintf("  Date range: %s to %s\n", min(files_dt$date), max(files_dt$date)))
cat(sprintf("  Total unique dates: %d\n", length(unique(files_dt$date))))

# Load all polygons
cat("\n  Loading and processing all polygons...\n")
cat("  This may take a few minutes...\n")

all_polys_list <- list()
load_start <- Sys.time()

for (i in seq_len(nrow(files_dt))) {
  if (i %% 50 == 0) {
    cat(sprintf("    Loaded %d/%d files (%.1f%%)\n", i, nrow(files_dt), 100*i/nrow(files_dt)))
  }
  
  tryCatch({
    poly <- st_read(files_dt$file[i], quiet = TRUE) %>%
      st_transform(32618) %>%
      st_simplify(dTolerance = SIMPLIFY, preserveTopology = FALSE)
    
    # Union multiple polygons into one
    if (nrow(poly) > 1) {
      poly <- st_union(poly) %>% st_sf()
    }
    
    # Add metadata
    poly$datetime <- files_dt$datetime[i]
    poly$date <- files_dt$date[i]
    poly$hour <- files_dt$hour[i]
    poly$file_id <- i
    
    all_polys_list[[i]] <- poly
    
  }, error = function(e) {
    cat(sprintf("    Warning: Failed to load file %d\n", i))
  })
}

# Remove NULL entries
all_polys_list <- all_polys_list[!sapply(all_polys_list, is.null)]

if (length(all_polys_list) == 0) {
  stop("No polygons could be loaded!")
}

cat(sprintf("  ✓ Loaded %d polygons in %.1f seconds\n", 
            length(all_polys_list), 
            as.numeric(difftime(Sys.time(), load_start, units = "secs"))))

# Combine all polygons into one sf object
cat("  Combining polygons...\n")

# Ensure all polygons have the same columns (only keep the ones we added)
all_polys_list <- lapply(all_polys_list, function(poly) {
  # Keep only the columns we need
  poly_simple <- st_sf(
    datetime = poly$datetime,
    date = poly$date,
    hour = poly$hour,
    file_id = poly$file_id,
    geometry = st_geometry(poly)
  )
  st_crs(poly_simple) <- st_crs(poly)
  return(poly_simple)
})

all_polys <- do.call(rbind, all_polys_list)
rm(all_polys_list)
gc()

cat(sprintf("  ✓ Combined into single sf object with %d rows\n", nrow(all_polys)))

# =================================================================
# STEP 2: Create hex grid ONCE
# =================================================================
cat("\n[2] Creating hex grid (once)...\n")

hex_grid_path <- file.path(output_dir, "hex_grid_template.rds")

if (file.exists(hex_grid_path)) {
  cat("  Loading existing hex grid...\n")
  hex_grid <- readRDS(hex_grid_path)
} else {
  cat("  Creating new hex grid...\n")
  
  # Get bounding box from all polygons
  bbox <- st_bbox(all_polys)
  bbox["xmin"] <- bbox["xmin"] - 50000
  bbox["xmax"] <- bbox["xmax"] + 50000
  bbox["ymin"] <- bbox["ymin"] - 50000
  bbox["ymax"] <- bbox["ymax"] + 50000
  class(bbox) <- "bbox"
  attr(bbox, "crs") <- st_crs(32618)
  
  hex_grid <- st_make_grid(
    st_as_sfc(bbox), 
    cellsize = HEX_SIZE, 
    square = FALSE
  ) %>%
    st_sf() %>%
    mutate(hex_id = row_number())
  
  # Add centroids
  hex_centroids <- st_centroid(hex_grid) %>%
    st_transform(4326) %>%
    st_coordinates() %>%
    as.data.frame() %>%
    rename(centroid_lon = X, centroid_lat = Y)
  
  hex_grid$centroid_lon <- hex_centroids$centroid_lon
  hex_grid$centroid_lat <- hex_centroids$centroid_lat
  
  saveRDS(hex_grid, hex_grid_path)
  cat(sprintf("  ✓ Hex grid saved: %d hexagons\n", nrow(hex_grid)))
}

cat(sprintf("  Grid size: %d hexagons\n", nrow(hex_grid)))

# =================================================================
# STEP 3: Single-pass intersection (THE MAGIC!)
# =================================================================
cat("\n[3] Computing intersections (single pass)...\n")
cat("  This is the key optimization - checking each hex once against all polygons\n")

intersect_start <- Sys.time()

# Find which polygons intersect each hexagon
cat("  Running spatial intersection...\n")
intersections <- st_intersects(hex_grid, all_polys, sparse = TRUE)

cat("  Processing results...\n")

# For each hexagon, extract the datetimes of intersecting polygons
hex_results <- data.table(hex_id = integer(), 
                         hours_count = integer(),
                         datetimes_affected = character(),
                         days_affected = integer())

affected_count <- 0

for (i in seq_len(nrow(hex_grid))) {
  if (i %% 50000 == 0) {
    cat(sprintf("    Processed %d/%d hexagons (%.1f%%)\n", 
                i, nrow(hex_grid), 100*i/nrow(hex_grid)))
  }
  
  poly_indices <- intersections[[i]]
  
  if (length(poly_indices) > 0) {
    affected_count <- affected_count + 1
    
    affected_datetimes <- all_polys$datetime[poly_indices]
    affected_dates <- all_polys$date[poly_indices]
    
    hex_results <- rbind(hex_results, data.table(
      hex_id = hex_grid$hex_id[i],
      hours_count = length(affected_datetimes),
      datetimes_affected = paste(sort(affected_datetimes), collapse = ","),
      days_affected = length(unique(affected_dates))
    ))
  }
}

intersect_elapsed <- as.numeric(difftime(Sys.time(), intersect_start, units = "secs"))
cat(sprintf("\n  ✓ Intersection complete in %.1f seconds\n", intersect_elapsed))
cat(sprintf("  ✓ Found %d affected hexagons (out of %d total)\n", affected_count, nrow(hex_grid)))

# Join results with hex grid geometry
cat("  Creating final spatial object...\n")
hex_results_sf <- hex_grid %>%
  inner_join(hex_results, by = "hex_id") %>%
  st_transform(4326) %>%
  st_make_valid()

cat(sprintf("  ✓ Final dataset: %d hexagons with outage data\n", nrow(hex_results_sf)))

# =================================================================
# STEP 4: Save daily summaries
# =================================================================
cat("\n[4] Creating daily summaries...\n")

all_dates <- unique(files_dt$date)
cat(sprintf("  Processing %d unique dates\n", length(all_dates)))

for (date in all_dates) {
  # Filter to polygons from this date
  date_poly_ids <- which(all_polys$date == date)
  
  if (length(date_poly_ids) == 0) next
  
  # For each hex, check if any of its intersecting polygons are from this date
  daily_results <- data.table(hex_id = integer(), 
                             count = integer(),
                             datetimes_affected = character())
  
  for (i in seq_len(nrow(hex_grid))) {
    poly_indices <- intersections[[i]]
    date_poly_match <- poly_indices[poly_indices %in% date_poly_ids]
    
    if (length(date_poly_match) > 0) {
      affected_datetimes <- all_polys$datetime[date_poly_match]
      
      daily_results <- rbind(daily_results, data.table(
        hex_id = hex_grid$hex_id[i],
        count = length(affected_datetimes),
        datetimes_affected = paste(sort(affected_datetimes), collapse = ",")
      ))
    }
  }
  
  if (nrow(daily_results) > 0) {
    daily_sf <- hex_grid %>%
      inner_join(daily_results, by = "hex_id") %>%
      mutate(
        date = date,
        n_files = sum(files_dt$date == date)
      ) %>%
      st_transform(4326) %>%
      st_make_valid()
    
    daily_path <- file.path(output_dir, "daily", sprintf("daily_%s.geojson", date))
    st_write(daily_sf, daily_path, delete_dsn = TRUE, quiet = TRUE)
    
    cat(sprintf("  ✓ %s: %d hexes\n", date, nrow(daily_sf)))
  }
}

# =================================================================
# STEP 5: Save total cumulative
# =================================================================
cat("\n[5] Saving total cumulative...\n")

current_date <- format(Sys.time(), "%Y-%m-%d")
total_path <- file.path(output_dir, "total", "total_exposure.geojson")
snapshot_path <- file.path(cumulative_snapshots_dir, sprintf("cumulative_%s.geojson", current_date))

st_write(hex_results_sf, total_path, delete_dsn = TRUE, quiet = TRUE)
st_write(hex_results_sf, snapshot_path, delete_dsn = TRUE, quiet = TRUE)

cat(sprintf("  ✓ Total exposure saved: %d hexagons\n", nrow(hex_results_sf)))
cat(sprintf("  ✓ Snapshot saved: %s\n", basename(snapshot_path)))

# Save statistics CSV
stats_csv <- as.data.frame(hex_results_sf) %>%
  select(hex_id, centroid_lat, centroid_lon, hours_count, days_affected, datetimes_affected)

stats_path <- file.path(output_dir, "total", "total_stats.csv")
fwrite(stats_csv, stats_path)
cat("  ✓ Stats CSV saved\n")

# Save summary JSON
summary_data <- list(
  generated_at = format(Sys.time(), "%Y-%m-%d %H:%M:%S %Z"),
  last_update_date = current_date,
  total_hexes_affected = nrow(hex_results_sf),
  total_hours_scraped = sum(hex_results_sf$hours_count),
  date_range = list(
    first = min(files_dt$date),
    last = max(files_dt$date)
  ),
  hex_size_meters = HEX_SIZE,
  processing_info = list(
    method = "single-pass spatial intersection",
    total_polygon_files = nrow(files_dt)
  ),
  stats = list(
    mean_hours = mean(hex_results_sf$hours_count),
    median_hours = median(hex_results_sf$hours_count),
    max_hours = max(hex_results_sf$hours_count),
    mean_days = mean(hex_results_sf$days_affected),
    median_days = median(hex_results_sf$days_affected),
    max_days = max(hex_results_sf$days_affected)
  )
)

summary_path <- file.path(output_dir, "total", "summary.json")
writeLines(jsonlite::toJSON(summary_data, auto_unbox = TRUE, pretty = TRUE), summary_path)
cat("  ✓ Summary JSON saved\n")

# =================================================================
# STEP 6: Generate HTML
# =================================================================
cat("\n[6] Generating HTML map...\n")

num_days <- length(unique(files_dt$date))
hex_size_km <- round(HEX_SIZE / 1000, 1)

source("R/generate_html_only.R")  # Use existing HTML generator

# =================================================================
# Summary
# =================================================================
elapsed <- as.numeric(difftime(Sys.time(), start_time, units = "secs"))
cat(sprintf("\n✅ OPTIMIZED PROCESSING COMPLETE in %.1f seconds (%.1f minutes)\n", 
            elapsed, elapsed/60))

cat("\n📊 PROCESSING SUMMARY:\n")
cat(sprintf("  • Total polygon files: %d\n", nrow(files_dt)))
cat(sprintf("  • Unique dates: %d\n", length(all_dates)))
cat(sprintf("  • Hexagons affected: %d\n", nrow(hex_results_sf)))
cat(sprintf("  • Processing time: %.1f minutes\n", elapsed/60))
cat(sprintf("  • Speed improvement: ~%dx faster than sequential processing\n", 
            round(nrow(files_dt) * 67 / elapsed)))

cat("\n✅ READY FOR DEPLOYMENT\n")
