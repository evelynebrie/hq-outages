# HQ Outages - 15-MINUTE SCRAPE VERSION with Timestamp Tracking
# FIXED VERSION: Better detection of small outage polygons
# Adapted for data scraped every ~15 minutes with exact timestamps in filenames
# Tracks: Each time a hex appears in an outage file, its score increases by 1
#
# IMPORTANT: This script uses POLYGON files (polygons_*.geojson) which contain
# the actual outage areas, NOT the joined point files (outages_joined_full_*.geojson)

suppressPackageStartupMessages({
  library(sf)
  library(dplyr)
  library(jsonlite)
  library(data.table)
})

cat("=== HQ Outages 15-Minute Scrape Analysis System ===\n")
cat("Features:\n")
cat("  • Processes files with exact timestamps (YYYYMMDDTHHMMSS)\n")
cat("  • Uses POLYGON files for accurate area coverage\n")
cat("  • FIXED: Buffers small polygons to ensure detection\n")
cat("  • Tracks cumulative outage frequency per hex\n")
cat("  • Each hex appearance = +1 to total score\n")
cat("  • Generates daily and monthly summaries\n")
cat("  • Creates cumulative snapshots for time series analysis\n")
cat("  • Shows CURRENT outages from most recent reading\n")
cat(sprintf("\nStarting at: %s\n", format(Sys.time(), "%Y-%m-%d %H:%M:%S")))
start_time <- Sys.time()

# ==================================================================
# CONFIGURATION
# ==================================================================
data_path <- "data/daily"  # Input: polygon files
output_dir <- "public"     # Output: processed summaries
cumulative_snapshots_dir <- file.path(output_dir, "cumulative_snapshots")
cache_dir <- "cache"       # Cache: processed data to avoid re-reading
cache_file <- file.path(cache_dir, "cumulative_hex_data.rds")

HEX_SIZE <- 1000     # Hex size in meters
SIMPLIFY <- 0        # No simplification - preserves original geometry for maximum accuracy
BUFFER_SMALL_POLYS <- 0  # No buffer - keeps original outage sizes for credibility (10m stays 10m)
MAX_FILES_PER_RUN <- 1000  # Process max 1000 files per run, then deploy. Next run continues from where it left off.

# DATE RANGE FILTER: Process only files from this date onwards (to fill gaps)
DATE_FILTER_MIN <- "2026-02-15"  # Start date (inclusive)
DATE_FILTER_MAX <- NULL  # No end date - process all files from MIN onwards

# QUICK DEPLOY MODE: Set via environment variable to skip file processing
QUICK_DEPLOY <- Sys.getenv("QUICK_DEPLOY", "false") == "true"

# Create output directories
dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(file.path(output_dir, "daily"), recursive = TRUE, showWarnings = FALSE)
dir.create(file.path(output_dir, "monthly"), recursive = TRUE, showWarnings = FALSE)
dir.create(file.path(output_dir, "total"), recursive = TRUE, showWarnings = FALSE)
dir.create(cumulative_snapshots_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(cache_dir, recursive = TRUE, showWarnings = FALSE)

# ==================================================================
# STEP 1: PARSE ALL FILES WITH TIMESTAMPS
# ==================================================================

if (QUICK_DEPLOY) {
  cat("\n[QUICK DEPLOY MODE] Skipping file scanning - using cache only\n")

  # Must have cache in quick deploy mode
  if (!file.exists(cache_file)) {
    stop("Quick deploy mode requires existing cache file at: ", cache_file)
  }

  # Load cache to get processed files list
  cat("[CACHE] Loading cache...\n")
  cached_data <- readRDS(cache_file)
  cumulative_hex_data <- cached_data$cumulative_hex_data
  processed_files <- cached_data$processed_files

  cat(sprintf("  ✓ Loaded cache with %d hexagons and %d processed files\n",
              length(cumulative_hex_data), length(processed_files)))

  # Derive most_recent_datetime from processed_files (HTML generation needs it;
  # the block that normally defines it (step 3.5) is skipped in QUICK_DEPLOY mode).
  qd_ts <- regmatches(processed_files,
                      regexpr("[0-9]{8}[Tt][0-9]{6}", processed_files))
  qd_max_ts <- max(qd_ts)
  most_recent_datetime <- sprintf("%s-%s-%s %s:%s:%s",
    substr(qd_max_ts, 1, 4), substr(qd_max_ts, 5, 6), substr(qd_max_ts, 7, 8),
    substr(qd_max_ts, 10, 11), substr(qd_max_ts, 12, 13), substr(qd_max_ts, 14, 15))
  cat(sprintf("  ✓ Most recent datetime (from cache): %s\n", most_recent_datetime))

  # Use processed files as the file list
  files <- processed_files
  files_to_process <- character()  # Nothing new to process

} else {
  cat("\n[1] Scanning and parsing input files...\n")

  # PRIORITY: Look for POLYGON files first (these contain the actual outage areas)
  # Pattern: polygons_YYYYMMDDTHHMMSS.geojson
  polygon_files <- list.files(data_path,
                              pattern = "^polygons_.*\\d{8}[Tt]\\d{6}.*\\.geojson$",
                              full.names = TRUE,
                              recursive = TRUE)

  cat(sprintf("  Found %d polygon files\n", length(polygon_files)))

  # Also check for joined files as fallback (these contain points, not ideal)
  joined_files <- list.files(data_path,
                             pattern = "^outages_joined_full_.*\\d{8}[Tt]\\d{6}.*\\.geojson$",
                             full.names = TRUE,
                             recursive = TRUE)

  cat(sprintf("  Found %d joined point files (fallback)\n", length(joined_files)))

  # Prefer polygon files - they have the actual outage AREAS
  if (length(polygon_files) > 0) {
    files <- polygon_files
    cat("  ✓ Using POLYGON files (recommended - contains outage areas)\n")
  } else if (length(joined_files) > 0) {
    files <- joined_files
    cat("  ⚠ Using joined point files as fallback (less accurate)\n")
    cat("    Note: Point files only show outage locations, not full affected areas\n")
  } else {
    cat("⚠️  No files found matching patterns. Looking for:\n")
    cat("   - polygons_YYYYMMDDTHHMMSS.geojson (preferred)\n")
    cat("   - outages_joined_full_YYYYMMDDTHHMMSS.geojson (fallback)\n")

    # Debug: show what files ARE in the directory
    all_files <- list.files(data_path, pattern = "\\.geojson$", full.names = FALSE, recursive = TRUE)
    if (length(all_files) > 0) {
      cat("\n  Files found in data directory:\n")
      cat(paste("   -", head(all_files, 10), collapse = "\n"), "\n")
      if (length(all_files) > 10) cat(sprintf("   ... and %d more\n", length(all_files) - 10))
    }

    stop("No input files found")
  }
}

# ==================================================================
# LOAD CACHE IF IT EXISTS (skip if already loaded in quick deploy mode)
# ==================================================================
if (!QUICK_DEPLOY) {
  cumulative_hex_data <- list()
  processed_files <- character()

  if (file.exists(cache_file)) {
    cat("\n[CACHE] Loading previously processed data...\n")
    cached_data <- readRDS(cache_file)
    cumulative_hex_data <- cached_data$cumulative_hex_data
    processed_files <- cached_data$processed_files
    cat(sprintf("  ✓ Loaded cache with %d hexagons and %d processed files\n",
                length(cumulative_hex_data), length(processed_files)))

  # Remove files in date filter range that have NO DATA in cumulative_hex_data
  # This clears corrupted/unprocessed files but keeps successfully processed ones
  if (!is.null(DATE_FILTER_MIN) && length(cumulative_hex_data) > 0) {
    max_desc <- if (is.null(DATE_FILTER_MAX)) "onwards" else DATE_FILTER_MAX
    cat(sprintf("\n🔧 VALIDATING cache for date range %s to %s...\n",
                DATE_FILTER_MIN, max_desc))

    # Get all dates that actually have data in the cache
    dates_with_data <- unique(unlist(lapply(cumulative_hex_data, function(x) x$dates)))

    cleared_count <- 0
    kept_files <- character()

    for (f in processed_files) {
      # Extract date from filename
      file_date <- sub(".*_(\\d{8})T.*", "\\1", basename(f))
      file_date <- paste0(substr(file_date, 1, 4), "-",
                         substr(file_date, 5, 6), "-",
                         substr(file_date, 7, 8))

      # Keep files if:
      # 1. Outside the date filter range, OR
      # 2. Inside the range AND has actual data in cache
      outside_range <- file_date < DATE_FILTER_MIN || (!is.null(DATE_FILTER_MAX) && file_date > DATE_FILTER_MAX)

      if (outside_range) {
        # Outside range - always keep
        kept_files <- c(kept_files, f)
      } else if (file_date %in% dates_with_data) {
        # Inside range but has data - keep it
        kept_files <- c(kept_files, f)
      } else {
        # Inside range with no data - clear it for reprocessing
        cleared_count <- cleared_count + 1
      }
    }

    if (cleared_count > 0) {
      cat(sprintf("  ✓ Cleared %d files without data in cache\n", cleared_count))
      cat(sprintf("  ✓ Kept %d files (outside range or with data)\n", length(kept_files)))
      processed_files <- kept_files
    } else {
      cat("  ✓ All files in range have data - no clearing needed\n")
    }
  }
}  # End of cache loading if block
}  # End of if (!QUICK_DEPLOY)

# Apply date range filter if specified (skip in quick deploy mode)
if (!QUICK_DEPLOY && !is.null(DATE_FILTER_MIN)) {
  max_desc <- if (is.null(DATE_FILTER_MAX)) "onwards" else paste("to", DATE_FILTER_MAX)
  cat(sprintf("\n[INFO]  DATE FILTER: Processing files from %s %s\n",
              DATE_FILTER_MIN, max_desc))

  # Extract dates from all files
  files_before_filter <- length(files)
  date_filtered_files <- character()

  for (f in files) {
    # Extract date from filename (YYYYMMDD)
    file_date <- sub(".*_(\\d{8})T.*", "\\1", basename(f))
    file_date <- paste0(substr(file_date, 1, 4), "-",
                       substr(file_date, 5, 6), "-",
                       substr(file_date, 7, 8))

    in_range <- file_date >= DATE_FILTER_MIN && (is.null(DATE_FILTER_MAX) || file_date <= DATE_FILTER_MAX)

    if (in_range) {
      date_filtered_files <- c(date_filtered_files, f)
    }
  }

  cat(sprintf("  • Files before filter: %d\n", files_before_filter))
  cat(sprintf("  • Files in date range: %d\n", length(date_filtered_files)))
  files <- date_filtered_files
}

# Filter to only new files (skip in quick deploy mode)
if (!QUICK_DEPLOY) {
  new_files <- setdiff(files, processed_files)

  if (length(new_files) == 0) {
    cat("\n[OK] All files already processed! Using cached data.\n")
    cat(sprintf("  • Total files: %d\n", length(files)))
    cat(sprintf("  • Already processed: %d\n", length(processed_files)))
    cat(sprintf("  • New files: 0\n"))
    cat("\nSkipping to summaries generation...\n")

    files_to_process <- character()
  } else {
    cat(sprintf("\n[INFO] File Summary:\n"))
    cat(sprintf("  • Total files found: %d\n", length(files)))
    cat(sprintf("  • Previously processed: %d\n", length(processed_files)))
    cat(sprintf("  • New files to process: %d\n", length(new_files)))

    files_to_process <- new_files
  }
} else {
  # Quick deploy: no new files to process, just regenerate outputs
  cat("\n[QUICK DEPLOY] Skipping file processing - regenerating outputs from cache\n")
  cat(sprintf("  • Cached hexagons: %d\n", length(cumulative_hex_data)))
  cat(sprintf("  • Cached files: %d\n", length(processed_files)))
}

# Parse timestamps for ALL files (needed for summaries)
basenames <- basename(files)
cat("  Sample filenames:\n")
cat(paste("   ", head(basenames, 3), collapse = "\n"), "\n")

# Extract timestamp part (YYYYMMDDTHHMMSS)
timestamps <- regmatches(basenames, 
                        regexpr("\\d{8}[Tt]\\d{6}", basenames, ignore.case = TRUE))

if (length(timestamps) == 0) {
  stop("[ERROR] Could not parse timestamps from filenames. Expected format: *_YYYYMMDDTHHMMSS.geojson")
}

# Parse into components
date_parts <- substr(timestamps, 1, 8)
time_parts <- substr(timestamps, 10, 15)

dates <- sprintf("%s-%s-%s", 
                substr(date_parts, 1, 4), 
                substr(date_parts, 5, 6), 
                substr(date_parts, 7, 8))

hours <- as.integer(substr(time_parts, 1, 2))
minutes <- as.integer(substr(time_parts, 3, 4))
seconds <- as.integer(substr(time_parts, 5, 6))

datetimes <- sprintf("%s %02d:%02d:%02d", dates, hours, minutes, seconds)

# Create file metadata table
files_dt <- data.table(
  file = files,
  date = dates,
  hour = hours,
  minute = minutes,
  datetime = datetimes,
  timestamp_sort = as.numeric(as.POSIXct(datetimes, format="%Y-%m-%d %H:%M:%S")),
  yearmon = format(as.Date(dates), "%Y-%m")
)

# Sort by timestamp
setorder(files_dt, timestamp_sort)

cat(sprintf("  Date range: %s to %s\n", min(dates), max(dates)))
cat(sprintf("  Total unique dates: %d\n", length(unique(dates))))
cat(sprintf("  Total unique months: %d\n", length(unique(files_dt$yearmon))))
cat(sprintf("  Files per day: min=%d, max=%d, mean=%.1f\n",
            min(table(dates)), max(table(dates)), mean(table(dates))))

# Deduplicate files within 10-minute window (handles duplicate scraping from AWS + GitHub)
if (nrow(files_dt) > 1) {
  files_to_keep <- logical(nrow(files_dt))
  files_to_keep[1] <- TRUE

  for (i in 2:nrow(files_dt)) {
    time_diff <- as.numeric(difftime(
      files_dt$timestamp_sort[i],
      files_dt$timestamp_sort[i-1],
      units = "mins"
    ))

    files_to_keep[i] <- (time_diff >= 10)
  }

  removed_count <- sum(!files_to_keep)
  if (removed_count > 0) {
    files_dt <- files_dt[files_to_keep, ]
    files <- files_dt$file
    cat(sprintf("  ⚠ Filtered out %d duplicate files (within 10 min of previous file)\n", removed_count))
  }
}

# ==================================================================
# STEP 2: CREATE OR LOAD HEX GRID TEMPLATE
# ==================================================================
cat("\n[2] Setting up hexagon grid reference...\n")

hex_grid_path <- file.path(output_dir, "hex_grid_template.rds")

if (file.exists(hex_grid_path)) {
  cat("  Loading existing hex grid reference...\n")
  hex_grid_reference <- readRDS(hex_grid_path)
} else {
  cat("  Creating new hex grid reference from first file...\n")
  
  first_file <- files_dt$file[1]
  first_data <- st_read(first_file, quiet = TRUE) %>% st_transform(32618)
  
  # Create expanded bounding box
  bbox <- st_bbox(first_data)
  bbox["xmin"] <- bbox["xmin"] - 50000
  bbox["xmax"] <- bbox["xmax"] + 50000
  bbox["ymin"] <- bbox["ymin"] - 50000
  bbox["ymax"] <- bbox["ymax"] + 50000
  class(bbox) <- "bbox"
  attr(bbox, "crs") <- st_crs(32618)
  
  # Create hex grid
  hex_grid_reference <- st_make_grid(
    st_as_sfc(bbox), 
    cellsize = HEX_SIZE, 
    square = FALSE
  ) %>%
    st_sf() %>%
    mutate(hex_id = row_number())
  
  # Add centroids in WGS84
  hex_centroids <- st_centroid(hex_grid_reference) %>%
    st_transform(4326) %>%
    st_coordinates() %>%
    as.data.frame() %>%
    rename(centroid_lon = X, centroid_lat = Y)
  
  hex_grid_reference$centroid_lon <- hex_centroids$centroid_lon
  hex_grid_reference$centroid_lat <- hex_centroids$centroid_lat
  
  saveRDS(hex_grid_reference, hex_grid_path)
  cat(sprintf("  ✓ Hex grid created and saved: %d total hexagons\n", nrow(hex_grid_reference)))
}

cat(sprintf("  Reference grid: %d hexagons\n", nrow(hex_grid_reference)))

# ==================================================================
# STEP 3: PROCESS NEW FILES AND UPDATE CUMULATIVE DATA
# ==================================================================
cat("\n[3] Processing files and tracking hex occurrences...\n")

# Track progress
if (length(files_to_process) == 0) {
  cat("  ✓ No new files to process (using cached data)\n")
  cat(sprintf("  ✓ Tracked %d unique hexagons with outages\n", length(cumulative_hex_data)))
} else {
  # Only process new files
  files_to_process_dt <- files_dt[files_dt$file %in% files_to_process, ]

  # BATCH PROCESSING: Limit to MAX_FILES_PER_RUN files per run for incremental deployment
  total_new_files <- nrow(files_to_process_dt)
  if (total_new_files > MAX_FILES_PER_RUN) {
    cat(sprintf("\n[INFO] BATCH PROCESSING: %d new files found, processing first %d this run\n",
                total_new_files, MAX_FILES_PER_RUN))
    cat(sprintf("   Remaining %d files will be processed in next run(s)\n",
                total_new_files - MAX_FILES_PER_RUN))
    files_to_process_dt <- files_to_process_dt[1:MAX_FILES_PER_RUN, ]
  } else {
    cat(sprintf("\n✓ Processing all %d new files (within limit of %d per run)\n",
                total_new_files, MAX_FILES_PER_RUN))
  }

  total_to_process <- nrow(files_to_process_dt)
  processed <- 0
  last_pct <- 0

  for (i in seq_len(total_to_process)) {
    f <- files_to_process_dt$file[i]
    date_val <- files_to_process_dt$date[i]
    datetime_val <- files_to_process_dt$datetime[i]
    
    # Progress indicator
    processed <- processed + 1
    pct <- round(100 * processed / total_to_process)
    if (pct >= last_pct + 10 || processed == total_to_process) {
      cat(sprintf("  Progress: %d%% (%d/%d files)\n", pct, processed, total_to_process))
      last_pct <- pct
    }
  
  tryCatch({
    # Read the file
    input_data <- st_read(f, quiet = TRUE) %>%
      st_transform(32618)
    
    # Check geometry type
    geom_types <- unique(st_geometry_type(input_data))
    cat(sprintf("    File %s: %d features, types: %s\n", 
                basename(f), nrow(input_data), paste(geom_types, collapse = ", ")))
    
    # Handle different geometry types
    if (any(grepl("POLYGON", geom_types))) {
      # It's polygon data - good! Union and find intersecting hexes
      polys <- input_data %>%
        st_simplify(dTolerance = SIMPLIFY, preserveTopology = FALSE)
      
      # FIXED: Buffer EACH small polygon BEFORE union
      # This ensures tiny polygons don't get lost
      hex_area <- HEX_SIZE * HEX_SIZE * 0.866  # Approximate hex area
      small_count <- 0
      
      for (j in 1:nrow(polys)) {
        poly_area <- as.numeric(st_area(polys[j, ]))
        # More aggressive: buffer if less than 3x hex area
        if (poly_area < (hex_area * 3)) {
          polys[j, ] <- st_buffer(polys[j, ], dist = BUFFER_SMALL_POLYS)
          small_count <- small_count + 1
        }
      }
      
      if (small_count > 0) {
        cat(sprintf("      (buffered %d small polygon%s)\n", small_count, 
                    ifelse(small_count > 1, "s", "")))
      }
      
      # Union all polygons AFTER buffering
      if (nrow(polys) > 1) {
        polys <- st_union(polys)
      }
      
      # Find affected hexagons (any hex that INTERSECTS with the outage polygons)
      hits <- st_intersects(hex_grid_reference, polys, sparse = TRUE)
      affected_ids <- which(lengths(hits) > 0)
      
    } else if (any(grepl("POINT", geom_types))) {
      # It's point data - less accurate, but we can buffer them
      cat("    ⚠ Point data detected - buffering points for area estimation\n")
      
      # Buffer points by 500m to create approximate outage areas
      points_buffered <- input_data %>%
        st_buffer(dist = 500) %>%
        st_union()
      
      hits <- st_intersects(hex_grid_reference, points_buffered, sparse = TRUE)
      affected_ids <- which(lengths(hits) > 0)
      
    } else {
      cat(sprintf("    ⚠ Unknown geometry type: %s, skipping\n", paste(geom_types, collapse = ", ")))
      next
    }
    
    cat(sprintf("    → %d hexagons affected\n", length(affected_ids)))
    
    # Update cumulative data for each affected hex
    for (hex_id in affected_ids) {
      hex_key <- as.character(hex_id)
      
      if (is.null(cumulative_hex_data[[hex_key]])) {
        # First time seeing this hex
        cumulative_hex_data[[hex_key]] <- list(
          count = 1,
          dates = date_val,
          datetimes = datetime_val
        )
      } else {
        # Increment count and add datetime
        cumulative_hex_data[[hex_key]]$count <- 
          cumulative_hex_data[[hex_key]]$count + 1
        cumulative_hex_data[[hex_key]]$dates <- 
          c(cumulative_hex_data[[hex_key]]$dates, date_val)
        cumulative_hex_data[[hex_key]]$datetimes <- 
          c(cumulative_hex_data[[hex_key]]$datetimes, datetime_val)
      }
    }
    
    rm(input_data)
    gc(verbose = FALSE)

  }, error = function(e) {
    cat(sprintf("    ⚠ Error processing file %s: %s\n", basename(f), e$message))
  })

  # INCREMENTAL SAVE: Save cache every 100 files (so progress isn't lost on timeout)
  if (processed %% 100 == 0) {
    cat(sprintf("\n[INCREMENTAL SAVE] Saving progress after %d files...\n", processed))
    cache_data_incremental <- list(
      cumulative_hex_data = cumulative_hex_data,
      processed_files = c(processed_files, files_to_process_dt$file[1:processed])
    )
    saveRDS(cache_data_incremental, cache_file)
    cat(sprintf("  ✓ Cache saved with %d hexagons and %d total files\n",
                length(cumulative_hex_data), length(cache_data_incremental$processed_files)))
  }
  }

  cat(sprintf("  ✓ Processed %d new files\n", processed))
  cat(sprintf("  ✓ Tracked %d unique hexagons with outages\n", length(cumulative_hex_data)))
}

# ==================================================================
# SAVE CACHE IMMEDIATELY (before summaries/HTML that might fail)
# ==================================================================
cat("\n[CACHE] Saving processed data (early save for safety)...\n")
cache_data <- list(
  cumulative_hex_data = cumulative_hex_data,
  processed_files = c(processed_files, files_to_process)
)
saveRDS(cache_data, cache_file)
cat(sprintf("  ✓ Cache saved with %d hexagons and %d total files\n",
            length(cumulative_hex_data), length(cache_data$processed_files)))
cat("  ✓ Cache is safe even if later steps fail!\n")

# Track which dates were updated (for incremental daily summary generation)
if (exists("files_to_process_dt") && nrow(files_to_process_dt) > 0) {
  updated_dates <- unique(files_to_process_dt$date)
  updated_months <- unique(files_to_process_dt$yearmon)
  cat(sprintf("\n[INFO] Data updated for %d dates and %d months\n",
              length(updated_dates), length(updated_months)))
} else {
  updated_dates <- character()
  updated_months <- character()
}

# ==================================================================
# STEP 3.5: GENERATE CURRENT OUTAGES (from most recent file)
# ==================================================================

# Skip in quick deploy mode (no data files available)
if (!QUICK_DEPLOY) {
  cat("\n[3.5] Generating current outages from most recent file...\n")

  # Get the most recent file
  most_recent_file <- files_dt$file[nrow(files_dt)]
  most_recent_datetime <- files_dt$datetime[nrow(files_dt)]
  cat(sprintf("  Most recent file: %s\n", basename(most_recent_file)))
  cat(sprintf("  Timestamp: %s\n", most_recent_datetime))

  # Store count for HTML generation
  current_outage_count <- 0

  # Process the most recent file to get current outages
  tryCatch({
  current_data <- st_read(most_recent_file, quiet = TRUE) %>%
    st_transform(32618)
  
  cat(sprintf("  Read %d features from file\n", nrow(current_data)))
  
  # Check geometry type
  geom_types <- unique(st_geometry_type(current_data))
  
  if (any(grepl("POLYGON", geom_types))) {
    # Polygon data
    current_polys <- current_data %>%
      st_simplify(dTolerance = SIMPLIFY, preserveTopology = FALSE)
    
    # FIXED: Buffer EACH small polygon BEFORE union
    hex_area <- HEX_SIZE * HEX_SIZE * 0.866
    small_count <- 0
    
    for (j in 1:nrow(current_polys)) {
      poly_area <- as.numeric(st_area(current_polys[j, ]))
      # More aggressive: buffer if less than 3x hex area
      if (poly_area < (hex_area * 3)) {
        current_polys[j, ] <- st_buffer(current_polys[j, ], dist = BUFFER_SMALL_POLYS)
        small_count <- small_count + 1
      }
    }
    
    if (small_count > 0) {
      cat(sprintf("  (buffered %d small current polygon%s)\n", small_count,
                  ifelse(small_count > 1, "s", "")))
    }
    
    # Union AFTER buffering
    if (nrow(current_polys) > 1) {
      current_polys <- st_union(current_polys)
    }
    
    current_hits <- st_intersects(hex_grid_reference, current_polys, sparse = TRUE)
    
  } else if (any(grepl("POINT", geom_types))) {
    # Point data - buffer
    current_polys <- current_data %>%
      st_buffer(dist = 500) %>%
      st_union()
    
    current_hits <- st_intersects(hex_grid_reference, current_polys, sparse = TRUE)
    
  } else {
    stop("Unknown geometry type")
  }
  
  current_affected_ids <- which(lengths(current_hits) > 0)
  
  current_outage_count <- length(current_affected_ids)
  cat(sprintf("  ✓ Current outages affect %d hexagons\n", current_outage_count))
  
  # Create current outages GeoJSON
  if (length(current_affected_ids) > 0) {
    current_summary <- hex_grid_reference[current_affected_ids, ]
    current_summary$is_current <- TRUE
    current_summary$last_reading <- most_recent_datetime
    
    # Add cumulative data for each current hex
    current_summary$total_occurrences <- sapply(current_affected_ids, function(id) {
      hex_key <- as.character(id)
      if (!is.null(cumulative_hex_data[[hex_key]])) {
        cumulative_hex_data[[hex_key]]$count
      } else {
        1
      }
    })
    
    current_summary$days_affected <- sapply(current_affected_ids, function(id) {
      hex_key <- as.character(id)
      if (!is.null(cumulative_hex_data[[hex_key]])) {
        length(unique(cumulative_hex_data[[hex_key]]$dates))
      } else {
        1
      }
    })
    
    current_output <- current_summary %>% st_transform(4326)
    
    output_file <- file.path(output_dir, "current.geojson")
    st_write(current_output, output_file, delete_dsn = TRUE, quiet = TRUE)
    cat(sprintf("  ✓ Current outages saved: %d hexagons\n", nrow(current_output)))
  } else {
    # No current outages - create empty GeoJSON
    cat("  Creating empty current.geojson (no active outages)\n")
    empty_geojson <- '{"type":"FeatureCollection","features":[]}'
    writeLines(empty_geojson, file.path(output_dir, "current.geojson"))
    cat("  ✓ Empty current.geojson created\n")
  }
  
  rm(current_data)
  gc(verbose = FALSE)
  
  }, error = function(e) {
    cat(sprintf("  ⚠ Error processing current outages: %s\n", e$message))
    empty_geojson <- '{"type":"FeatureCollection","features":[]}'
    writeLines(empty_geojson, file.path(output_dir, "current.geojson"))
  })
} else {
  # Quick deploy mode: skip current outages processing
  cat("\n[3.5] QUICK DEPLOY: Skipping current outages generation\n")
  cat("  Creating empty current.geojson\n")
  empty_geojson <- '{"type":"FeatureCollection","features":[]}'
  writeLines(empty_geojson, file.path(output_dir, "current.geojson"))
  current_outage_count <- 0
}

# ==================================================================
# STEP 4: GENERATE DAILY SUMMARIES (INCREMENTAL - ONLY UPDATED DATES)
# ==================================================================
cat("\n[4] Generating daily summaries...\n")

# FIXED: Get unique dates from PROCESSED cache, not all downloaded files
# This prevents generating empty daily files for dates that haven't been processed yet
unique_dates <- unique(unlist(lapply(cumulative_hex_data, function(x) x$dates)))
unique_dates <- sort(unique_dates)

# OPTIMIZATION: Only regenerate dates that were updated
if (length(updated_dates) > 0) {
  dates_to_generate <- updated_dates
  cat(sprintf("  INCREMENTAL: Only regenerating %d updated dates (out of %d total)\n",
              length(dates_to_generate), length(unique_dates)))
} else {
  # No new data - set to empty so all existing files get skipped
  dates_to_generate <- character()
  cat(sprintf("  No new data - will skip all %d dates with existing output files\n", length(unique_dates)))
}

# Build daily summaries from cumulative data
skipped_count <- 0
generated_count <- 0

for (date in unique_dates) {
  output_file <- file.path(output_dir, "daily", sprintf("daily_%s.geojson", date))

  # Skip if date wasn't updated and output file exists
  if (!date %in% dates_to_generate && file.exists(output_file)) {
    skipped_count <- skipped_count + 1
    next
  }

  cat(sprintf("  • %s...", date))
  generated_count <- generated_count + 1
  
  daily_hex_data <- list()
  
  for (hex_key in names(cumulative_hex_data)) {
    hex_info <- cumulative_hex_data[[hex_key]]
    date_mask <- hex_info$dates == date
    
    if (any(date_mask)) {
      daily_hex_data[[hex_key]] <- list(
        count = sum(date_mask),
        datetimes = hex_info$datetimes[date_mask]
      )
    }
  }
  
  if (length(daily_hex_data) > 0) {
    hex_ids <- as.integer(names(daily_hex_data))
    daily_summary <- hex_grid_reference[hex_ids, ]
    
    daily_summary$occurrences_today <- sapply(daily_hex_data, function(x) x$count)
    daily_summary$datetimes_today <- sapply(daily_hex_data, function(x) {
      paste(x$datetimes, collapse = ", ")
    })
    daily_summary$cumulative_total <- sapply(hex_ids, function(id) {
      cumulative_hex_data[[as.character(id)]]$count
    })
    
    daily_output <- daily_summary %>% st_transform(4326)
    
    output_file <- file.path(output_dir, "daily", sprintf("daily_%s.geojson", date))
    st_write(daily_output, output_file, delete_dsn = TRUE, quiet = TRUE)
    cat(" ✓\n")
  } else {
    cat(" (no data)\n")
  }
}

if (skipped_count > 0) {
  cat(sprintf("  ✓ Generated %d daily summaries, skipped %d unchanged\n",
              generated_count, skipped_count))
} else {
  cat(sprintf("  ✓ Generated daily summaries for %d dates\n", generated_count))
}

# ==================================================================
# STEP 5: GENERATE MONTHLY SUMMARIES (INCREMENTAL - ONLY UPDATED MONTHS)
# ==================================================================
cat("\n[5] Generating monthly summaries...\n")

unique_months <- unique(files_dt$yearmon)

# OPTIMIZATION: Only regenerate months that were updated
if (length(updated_months) > 0) {
  months_to_generate <- updated_months
  cat(sprintf("  INCREMENTAL: Only regenerating %d updated months (out of %d total)\n",
              length(months_to_generate), length(unique_months)))
} else {
  # No new data - set to empty so all existing files get skipped
  months_to_generate <- character()
  cat(sprintf("  No new data - will skip all %d months with existing output files\n", length(unique_months)))
}

monthly_skipped <- 0
monthly_generated <- 0

for (month in unique_months) {
  output_file <- file.path(output_dir, "monthly", sprintf("monthly_%s.geojson", month))

  # Skip if month wasn't updated and output file exists
  if (!month %in% months_to_generate && file.exists(output_file)) {
    monthly_skipped <- monthly_skipped + 1
    next
  }

  cat(sprintf("  • %s...", month))
  monthly_generated <- monthly_generated + 1
  
  month_dates <- unique(files_dt$date[files_dt$yearmon == month])
  monthly_hex_data <- list()
  
  for (hex_key in names(cumulative_hex_data)) {
    hex_info <- cumulative_hex_data[[hex_key]]
    month_mask <- hex_info$dates %in% month_dates
    
    if (any(month_mask)) {
      month_datetimes <- hex_info$datetimes[month_mask]
      month_dates_for_hex <- hex_info$dates[month_mask]
      
      monthly_hex_data[[hex_key]] <- list(
        count = sum(month_mask),
        dates = unique(month_dates_for_hex),
        datetimes = month_datetimes
      )
    }
  }
  
  if (length(monthly_hex_data) > 0) {
    hex_ids <- as.integer(names(monthly_hex_data))
    monthly_summary <- hex_grid_reference[hex_ids, ]
    
    monthly_summary$occurrences_month <- sapply(monthly_hex_data, function(x) x$count)
    monthly_summary$days_affected <- sapply(monthly_hex_data, function(x) {
      length(unique(x$dates))
    })
    monthly_summary$first_occurrence <- sapply(monthly_hex_data, function(x) {
      min(x$datetimes)
    })
    monthly_summary$last_occurrence <- sapply(monthly_hex_data, function(x) {
      max(x$datetimes)
    })
    
    monthly_output <- monthly_summary %>% st_transform(4326)
    
    output_file <- file.path(output_dir, "monthly", sprintf("monthly_%s.geojson", month))
    st_write(monthly_output, output_file, delete_dsn = TRUE, quiet = TRUE)
    cat(" ✓\n")
  } else {
    cat(" (no data)\n")
  }
}

if (monthly_skipped > 0) {
  cat(sprintf("  ✓ Generated %d monthly summaries, skipped %d unchanged\n",
              monthly_generated, monthly_skipped))
} else {
  cat(sprintf("  ✓ Generated monthly summaries for %d months\n", monthly_generated))
}

# ==================================================================
# STEP 6: GENERATE TOTAL CUMULATIVE SUMMARY
# ==================================================================
cat("\n[6] Generating cumulative total summary...\n")

hex_ids <- as.integer(names(cumulative_hex_data))
total_summary <- hex_grid_reference[hex_ids, ]

total_summary$total_occurrences <- sapply(cumulative_hex_data, function(x) x$count)
total_summary$days_affected <- sapply(cumulative_hex_data, function(x) {
  length(unique(x$dates))
})
total_summary$first_occurrence <- sapply(cumulative_hex_data, function(x) {
  min(x$datetimes)
})
total_summary$last_occurrence <- sapply(cumulative_hex_data, function(x) {
  max(x$datetimes)
})
total_summary$all_datetimes <- sapply(cumulative_hex_data, function(x) {
  paste(x$datetimes, collapse = ", ")
})

total_output <- total_summary %>% st_transform(4326)

# Split into regional files to stay under GitHub's 100 MB limit
cat("\n[INFO] Splitting into regional files...\n")

regions <- list(
  list(name = "west", lon_min = -79.54, lon_max = -75.0),
  list(name = "central_west", lon_min = -75.0, lon_max = -72.0),
  list(name = "central", lon_min = -72.0, lon_max = -69.0),
  list(name = "central_east", lon_min = -69.0, lon_max = -66.0),
  list(name = "east", lon_min = -66.0, lon_max = -63.0),
  list(name = "far_east", lon_min = -63.0, lon_max = -61.46)
)

for (i in seq_along(regions)) {
  region <- regions[[i]]
  is_last_region <- (i == length(regions))

  # Use <= for last region to include eastern boundary
  regional_data <- if (is_last_region) {
    total_output %>% filter(centroid_lon >= region$lon_min & centroid_lon <= region$lon_max)
  } else {
    total_output %>% filter(centroid_lon >= region$lon_min & centroid_lon < region$lon_max)
  }

  if (nrow(regional_data) > 0) {
    region_file <- file.path(output_dir, "total", sprintf("total_exposure_%s.geojson", region$name))
    st_write(regional_data, region_file, delete_dsn = TRUE, quiet = TRUE)
    cat(sprintf("  ✓ %s: %d hexagons\n", region$name, nrow(regional_data)))
  }
}

cat(sprintf("\n  ✓ Total hexagons affected: %d\n", nrow(total_output)))
cat(sprintf("  ✓ Max occurrences per hex: %d\n", max(total_summary$total_occurrences)))
cat(sprintf("  ✓ Mean occurrences per hex: %.1f\n", mean(total_summary$total_occurrences)))

# Save regional snapshots (same regions as total files)
cat("\n[INFO] Saving regional snapshots...\n")
snapshot_date <- format(Sys.Date(), "%Y%m%d")

for (i in seq_along(regions)) {
  region <- regions[[i]]
  is_last_region <- (i == length(regions))

  # Use <= for last region to include eastern boundary
  regional_data <- if (is_last_region) {
    total_output %>% filter(centroid_lon >= region$lon_min & centroid_lon <= region$lon_max)
  } else {
    total_output %>% filter(centroid_lon >= region$lon_min & centroid_lon < region$lon_max)
  }

  if (nrow(regional_data) > 0) {
    snapshot_file <- file.path(cumulative_snapshots_dir,
                              sprintf("cumulative_%s_%s.geojson", snapshot_date, region$name))
    st_write(regional_data, snapshot_file, delete_dsn = TRUE, quiet = TRUE)
    cat(sprintf("  ✓ %s: %d hexagons\n", region$name, nrow(regional_data)))
  }
}

# ==================================================================
# STEP 7: GENERATE HTML VISUALIZATION
# ==================================================================
cat("\n[7] Generating HTML visualization...\n")

# Calculate statistics from ACTUAL processed data, not downloaded files
num_days <- length(unique_dates)  # Use unique_dates from cumulative cache
hex_size_km <- sprintf("%.1f", HEX_SIZE / 1000)
current_date <- format(Sys.time(), "%Y-%m-%d %H:%M", tz = "America/Toronto")  # Montreal timezone
# French-formatted version for display: "22 avril 2026 à 14h52"
FR_MONTHS_R <- c("janvier","f\u00e9vrier","mars","avril","mai","juin","juillet","ao\u00fbt","septembre","octobre","novembre","d\u00e9cembre")
.now_parts <- as.POSIXlt(Sys.time(), tz = "America/Toronto")
current_date_fr <- sprintf("%d %s %d \u00e0 %02dh%02d",
                            .now_parts$mday,
                            FR_MONTHS_R[.now_parts$mon + 1],
                            .now_parts$year + 1900,
                            .now_parts$hour,
                            .now_parts$min)

# Build dates JSON array
dates_json <- toJSON(unique_dates)

# Write HTML file directly using cat() to avoid any escaping issues
html_file <- file(file.path(output_dir, "index.html"), "w")

cat('<!DOCTYPE html>
<html lang="fr">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Où sont les pannes?</title>
    <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
    <link rel="stylesheet" href="https://unpkg.com/leaflet-control-geocoder@2.4.0/dist/Control.Geocoder.css" />
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600&family=JetBrains+Mono:wght@500;600&display=swap" rel="stylesheet">
    <script data-goatcounter="https://ebudem.goatcounter.com/count"
            async src="//gc.zgo.at/count.js"></script>
    <style>
        * { box-sizing: border-box; }
        body { margin: 0; font-family: "Inter", -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; }
        #map { height: 100vh; width: 100%; }
        
        /* Main title banner - minimalist style */
        .main-title {
            position: absolute;
            top: 20px;
            left: 50%;
            transform: translateX(-50%);
            z-index: 1000;
            background: #ffffff;
            color: #000000;
            padding: 14px 28px;
            border-radius: 6px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.12);
            font-family: "JetBrains Mono", monospace;
            font-size: 15px;
            font-weight: 600;
            letter-spacing: 0.02em;
            text-align: center;
            border: 1px solid #e5e5e5;
        }
        
        /* Clean controls box */
        .controls {
            padding: 14px;
            background: rgba(255, 255, 255, 0.95);
            border-radius: 6px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.12);
            min-width: 220px;
            border: 1px solid #e5e5e5;
        }
        .controls h4 {
            margin: 0 0 10px 0;
            color: #1a1a1a;
            font-size: 14px;
            font-weight: 600;
        }
        .controls select {
            width: 100%;
            padding: 8px 10px;
            border: 1px solid #d4d4d4;
            border-radius: 4px;
            font-size: 13px;
            font-family: inherit;
            background: white;
            cursor: pointer;
            transition: border-color 0.2s;
            color: #1a1a1a;
        }
        .controls select:hover {
            border-color: #a3a3a3;
        }
        .toggle-data-btn {
            width: 100%;
            padding: 8px 10px;
            margin-top: 10px;
            background: #f3f4f6;
            border: 1px solid #d1d5db;
            border-radius: 6px;
            font-size: 13px;
            font-family: inherit;
            color: #1a1a1a;
            cursor: pointer;
            transition: background-color 0.2s, border-color 0.2s;
            font-weight: 500;
        }
        .toggle-data-btn:hover {
            background: #e5e7eb;
            border-color: #9ca3af;
        }
        .toggle-data-btn:active {
            background: #d1d5db;
        }
        .controls select:focus {
            outline: none;
            border-color: #525252;
        }
        
        /* Enhanced info box */
        .info {
            padding: 16px;
            background: white;
            border-radius: 12px;
            box-shadow: 0 4px 12px rgba(0,0,0,0.15);
            min-width: 260px;
        }
        .info h4 {
            margin: 0 0 14px 0;
            color: #000000;
            font-size: 16px;
            font-weight: 600;
            padding-bottom: 10px;
            border-bottom: 2px solid #e5e7eb;
        }
        .info-row {
            display: flex;
            justify-content: space-between;
            align-items: center;
            gap: 20px;
            padding: 8px 0;
            border-bottom: 1px solid #f3f4f6;
        }
        .info-row:last-child {
            border-bottom: none;
        }
        .info-label {
            font-weight: 500;
            color: #6b7280;
            font-size: 13px;
        }
        .info-value {
            font-weight: 600;
            color: #111827;
            font-size: 14px;
        }
        
        /* Legend styling */
        .legend {
            line-height: 24px;
            color: #374151;
            padding: 16px;
            background: white;
            border-radius: 12px;
            box-shadow: 0 4px 12px rgba(0,0,0,0.15);
        }
        .legend h4 {
            margin: 0 0 12px 0;
            color: #000000;
            font-size: 16px;
            font-weight: 600;
        }
        .legend i {
            width: 20px;
            height: 20px;
            float: left;
            margin-right: 10px;
            opacity: 0.85;
            border-radius: 3px;
        }
        .legend-item {
            display: flex;
            align-items: center;
            margin-bottom: 6px;
            font-size: 13px;
        }
        .legend-info-wrap {
            position: relative;
            display: flex;
            justify-content: flex-end;
            margin-top: 10px;
            padding-top: 8px;
            border-top: 1px solid #e5e7eb;
        }
        .legend-info-btn {
            width: 22px; height: 22px;
            border-radius: 50%;
            background: #e5e7eb;
            color: #374151;
            border: none;
            cursor: pointer;
            font-style: italic;
            font-family: Georgia, "Times New Roman", serif;
            font-weight: bold;
            font-size: 13px;
            display: flex; align-items: center; justify-content: center;
            padding: 0; line-height: 1;
            transition: background 0.15s;
        }
        .legend-info-btn:hover { background: #d1d5db; }
        .legend-info-popover {
            display: none;
            position: absolute;
            right: 0;
            bottom: 30px;
            width: 280px;
            background: white;
            color: #374151;
            font-size: 11px;
            line-height: 1.5;
            padding: 10px 12px;
            border: 1px solid #e5e7eb;
            border-radius: 6px;
            box-shadow: 0 4px 12px rgba(0,0,0,0.12);
            z-index: 1500;
        }
        .legend-info-popover.open { display: block; }
        
        /* Enhanced badges - bright yellow */
        .current-badge {
            background: #f5d05d;
            color: #422006;
            padding: 4px 10px;
            border-radius: 6px;
            font-size: 12px;
            font-weight: 600;
            letter-spacing: 0.02em;
        }
        
        /* Modal styling */
        .detail-link {
            color: #2563eb;
            cursor: pointer;
            text-decoration: none;
            font-weight: 500;
            transition: color 0.2s;
        }
        .detail-link:hover {
            color: #1e40af;
            text-decoration: underline;
        }
        #detailModal {
            position: fixed;
            top: 50%;
            left: 50%;
            transform: translate(-50%, -50%);
            background: white;
            padding: 24px;
            border-radius: 16px;
            box-shadow: 0 20px 60px rgba(0,0,0,0.3);
            max-height: 80vh;
            overflow-y: auto;
            z-index: 10000;
            display: none;
            min-width: 320px;
            max-width: 500px;
        }
        #detailModal.active { display: block; }
        #detailModal h3 {
            margin: 0 0 16px 0;
            color: #1e40af;
            font-size: 20px;
        }
        #modalOverlay {
            position: fixed;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
            background: rgba(0,0,0,0.6);
            z-index: 9999;
            display: none;
            backdrop-filter: blur(2px);
        }
        #modalOverlay.active { display: block; }

        /* Info button (FAQ trigger) */
        .info-btn {
            position: absolute;
            bottom: 30px;
            left: 10px;
            width: 36px;
            height: 36px;
            border-radius: 50%;
            background: white;
            border: none;
            box-shadow: 0 4px 12px rgba(0,0,0,0.15);
            cursor: pointer;
            font-family: Georgia, "Times New Roman", serif;
            font-size: 20px;
            font-style: italic;
            font-weight: bold;
            color: #374151;
            display: flex;
            align-items: center;
            justify-content: center;
            z-index: 1000;
            transition: transform 0.15s, box-shadow 0.15s;
            padding: 0;
        }
        .info-btn:hover {
            transform: scale(1.08);
            box-shadow: 0 6px 16px rgba(0,0,0,0.2);
        }
        #faqModal {
            position: fixed;
            top: 50%;
            left: 50%;
            transform: translate(-50%, -50%);
            background: white;
            padding: 28px;
            border-radius: 16px;
            box-shadow: 0 20px 60px rgba(0,0,0,0.3);
            max-height: 80vh;
            overflow-y: auto;
            z-index: 10000;
            display: none;
            min-width: 320px;
            max-width: 640px;
        }
        #faqModal.active { display: block; }
        #faqModal h3 {
            margin: 0 0 16px 0;
            color: #111827;
            font-size: 20px;
            font-weight: 700;
        }

        /* About block — prose intro above the FAQ */
        #faqModal .about-block { margin: 0 0 22px; }
        #faqModal .about-block p {
            margin: 0 0 12px;
            color: #374151;
            font-size: 13.5px;
            line-height: 1.6;
        }
        #faqModal .about-block p:last-child { margin-bottom: 0; }
        #faqModal .about-block strong { color: #111827; font-weight: 700; }
        #faqModal .about-block a {
            color: #78350f;
            background: #fef3c7;
            padding: 1px 6px;
            border-radius: 4px;
            text-decoration: none;
            font-weight: 500;
        }
        #faqModal .about-block a:hover { background: #fde68a; }

        /* Small all-caps label above the FAQ items */
        #faqModal .faq-section-title {
            font-size: 11px;
            font-weight: 600;
            color: #9ca3af;
            text-transform: uppercase;
            letter-spacing: 0.08em;
            margin: 0 0 10px;
        }

        /* Collapsible FAQ items */
        #faqModal details.faq-item {
            border-top: 1px solid #e5e7eb;
            padding: 10px 0;
        }
        #faqModal details.faq-item:last-of-type { border-bottom: 1px solid #e5e7eb; }
        #faqModal details.faq-item summary {
            cursor: pointer;
            font-weight: 600;
            font-size: 13.5px;
            color: #111827;
            list-style: none;
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 4px 0;
        }
        #faqModal details.faq-item summary::-webkit-details-marker { display: none; }
        #faqModal details.faq-item summary::after {
            content: "+";
            font-size: 18px;
            color: #6b7280;
            font-weight: 400;
            line-height: 1;
            margin-left: 10px;
        }
        #faqModal details.faq-item[open] summary::after { content: "–"; }
        #faqModal details.faq-item summary:hover { color: #78350f; }
        #faqModal details.faq-item p { margin: 8px 0 4px; color: #4b5563; font-size: 13px; line-height: 1.55; }

        /* Welcome modal */
        #welcomeModal {
            position: fixed;
            top: 50%; left: 50%;
            transform: translate(-50%, -50%);
            background: white;
            padding: 32px 36px;
            border-radius: 16px;
            box-shadow: 0 20px 60px rgba(0,0,0,0.3);
            max-height: 85vh;
            overflow-y: auto;
            z-index: 10000;
            display: none;
            max-width: 560px;
            width: 90%;
            text-align: center;
            box-sizing: border-box;
        }
        #welcomeModal.active { display: block; }
        #welcomeModal h2 {
            margin: 0 0 18px;
            color: #111827;
            font-size: 24px;
            font-weight: 700;
        }
        #welcomeModal p {
            margin: 0 0 14px;
            color: #374151;
            font-size: 14px;
            line-height: 1.6;
            text-align: left;
        }
        .welcome-actions {
            display: flex;
            gap: 10px;
            justify-content: center;
            margin: 22px 0 18px;
        }
        .btn-secondary, .btn-primary {
            padding: 10px 20px;
            border-radius: 8px;
            font-family: inherit;
            font-size: 14px;
            font-weight: 600;
            cursor: pointer;
            transition: all 0.15s;
            border: 2px solid #111827;
        }
        .btn-secondary { background: white; color: #111827; }
        .btn-secondary:hover { background: #f3f4f6; }
        .btn-primary { background: #111827; color: white; }
        .btn-primary:hover { background: #374151; border-color: #374151; }
        .welcome-logos {
            display: flex;
            align-items: center;
            justify-content: center;
            gap: 32px;
            padding-top: 18px;
            border-top: 1px solid #e5e7eb;
            flex-wrap: wrap;
        }
        .welcome-logos img {
            max-height: 42px;
            max-width: 140px;
            object-fit: contain;
            opacity: 0.9;
        }
        .visitor-pill {
            display: none;
            align-items: center;
            gap: 7px;
            background: linear-gradient(135deg, #fef3c7 0%, #fde68a 100%);
            border: 1px solid #fcd34d;
            color: #78350f;
            padding: 6px 14px;
            border-radius: 999px;
            font-size: 12.5px;
            font-weight: 500;
            margin: 0 auto 16px;
        }
        .visitor-pill.visible { display: inline-flex; }
        .visitor-pill::before {
            content: "";
            width: 7px; height: 7px;
            background: #f59e0b;
            border-radius: 50%;
            box-shadow: 0 0 0 3px rgba(245, 158, 11, 0.25);
            flex-shrink: 0;
        }
        .visitor-pill .num {
            color: #78350f;
            font-weight: 700;
            font-variant-numeric: tabular-nums;
        }

        /* Inline spinner shown inside the welcome button while data loads */
        .btn-spinner {
            display: inline-block;
            width: 14px; height: 14px;
            border: 2px solid rgba(255,255,255,0.35);
            border-top-color: white;
            border-radius: 50%;
            animation: btn-spin 0.8s linear infinite;
            vertical-align: -2px;
            margin-right: 8px;
        }
        @keyframes btn-spin { to { transform: rotate(360deg); } }
        .btn-primary[disabled] { cursor: wait; opacity: 0.9; }

        .date-row {
            margin: 8px 0;
            border-bottom: 1px solid #f3f4f6;
            padding: 8px 0;
        }
        .date-header {
            cursor: pointer;
            font-weight: 600;
            color: #111827;
            padding: 6px;
            border-radius: 6px;
            transition: background 0.2s;
        }
        .date-header:hover {
            background: #f3f4f6;
        }
        .times-list {
            display: none;
            margin-left: 24px;
            margin-top: 8px;
            font-size: 13px;
            color: #6b7280;
        }
        .times-list div {
            padding: 4px 0;
        }
        
        .loading {
            color: #9ca3af;
            font-style: italic;
            font-size: 13px;
        }
        
        /* Close button */
        .close-btn {
            float: right;
            border: none;
            background: #f3f4f6;
            width: 32px;
            height: 32px;
            border-radius: 8px;
            font-size: 18px;
            cursor: pointer;
            transition: all 0.2s;
            display: flex;
            align-items: center;
            justify-content: center;
        }
        .close-btn:hover {
            background: #e5e7eb;
            transform: scale(1.05);
        }
    </style>
</head>
<body>
    <div class="main-title">Cartographie des pannes au Québec</div>
    <div id="map"></div>
    <button class="info-btn" onclick="openFaqModal()" title="À propos de la carte" aria-label="Informations">i</button>

    <div id="modalOverlay" class="active" onclick="closeAllModals()"></div>
    <div id="welcomeModal" class="active">
        <button onclick="closeWelcomeModal()" class="close-btn">✕</button>
        <h2>Où sont les pannes?</h2>
        <p>Cet outil vous permet de localiser les pannes de courant ayant eu lieu au Québec depuis le 1<sup>er</sup> janvier 2026.</p>
        <p>L&apos;objectif de cette carte est de promouvoir la transparence de l&apos;information au sujet des pannes de courant au Québec. Chaque occurrence de panne représente une panne de 15 minutes.</p>
        <div class="welcome-actions">
            <button class="btn-secondary" onclick="openFaqFromWelcome()">À propos de ce projet</button>
            <button class="btn-primary" id="welcomeStartBtn" onclick="startFromWelcome()">Accéder à l&apos;outil</button>
        </div>
        <div id="visitorPill" class="visitor-pill">
            Vous êtes le <span class="num"><span id="visitorCount">&mdash;</span><sup>e</sup></span> visiteur depuis le lancement
        </div>
        <div class="welcome-logos">
            <img src="logos/ivado-rgb_logo-full-degrade.png" alt="IVADO">
            <img src="logos/udem-logo.svg" alt="Université de Montréal">
        </div>
    </div>
    <div id="detailModal">
        <button onclick="closeDetailModal()" class="close-btn">✕</button>
        <div id="modalContent"></div>
    </div>
    <div id="faqModal">
        <button onclick="closeFaqModal()" class="close-btn">✕</button>
        <h3>À propos de ce projet</h3>

        <div class="about-block">
            <p>Ce projet est dirigé par <strong>Evelyne Brie</strong>, professeure adjointe en science politique affiliée à IVADO à l&apos;Université de Montréal. Vous pouvez la contacter par courriel au <a href="mailto:evelyne.brie@umontreal.ca">evelyne.brie@umontreal.ca</a>.</p>
            <p>Ses recherches portent sur la cohésion sociale, les inégalités structurelles et la prise de décision en matière de politique urbaine. «&nbsp;Où sont les pannes?&nbsp;» fait partie de ce travail et vise à mettre en lumière les disparités en matière de coupures électriques au Québec.</p>
        </div>

        <div class="faq-section-title">Questions fréquentes</div>

        <details class="faq-item" open>
            <summary>D&apos;où proviennent ces données?</summary>
            <p>Les données sont collectées à chaque 15 minutes du site Info-Pannes d&apos;Hydro-Québec. Elles sont ensuite converties sur une grille hexagonale pour permettre de calculer les pannes cumulatives sur des aires géographiques d&apos;un kilomètre.</p>
        </details>

        <details class="faq-item">
            <summary>Qu&apos;est-ce qui constitue une occurrence de panne?</summary>
            <p>Une occurrence de panne est une lecture de panne, mesurée à chaque 15 minutes, via le site d&apos;Info-Pannes d&apos;Hydro-Québec. Par exemple, pour une coupure électrique d&apos;une durée totale d&apos;une heure, 4 occurrences sont comptabilisées. Cette technique permet de comparer la durée totale des pannes, plutôt que de comptabiliser des coupures de courant de durées différentes.</p>
        </details>

        <details class="faq-item">
            <summary>Une panne est indiquée dans l&apos;hexagone où je réside, mais mon logement n&apos;est pas affecté. S&apos;agit-il d&apos;une erreur?</summary>
            <p>Non. Les hexagones indiquent l&apos;occurrence d&apos;une panne à n&apos;importe quel endroit au sein de cet hexagone. Autrement dit, si vos voisins sont affectés par une panne sans que vous le soyez, l&apos;historique des pannes indiquera une occurrence sans que vous ayez été affecté.</p>
        </details>

        <details class="faq-item">
            <summary>Dans quel but cette carte a-t-elle été créée?</summary>
            <p>L&apos;objectif de cette carte est de promouvoir la transparence de l&apos;information au sujet des pannes de courant au Québec. L&apos;outil actuel d&apos;Info-Pannes d&apos;Hydro-Québec permet seulement de visualiser les pannes actuelles ou planifiées, sans partager d&apos;information sur les pannes résolues.</p>
        </details>
    </div>
    
    <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
    <script src="https://unpkg.com/leaflet-control-geocoder@2.4.0/dist/Control.Geocoder.js"></script>
    <script>
        var map = L.map("map").setView([45.75, -73.35], 10);
        L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
            attribution: "OpenStreetMap"
        }).addTo(map);

        // Add address search bar
        L.Control.geocoder({
            defaultMarkGeocode: false,
            placeholder: "Rechercher une adresse...",
            errorMessage: "Adresse non trouvée",
            position: "topleft"
        }).on("markgeocode", function(e) {
            var bbox = e.geocode.bbox;
            var poly = L.polygon([
                bbox.getSouthEast(),
                bbox.getNorthEast(),
                bbox.getNorthWest(),
                bbox.getSouthWest()
            ]);
            map.fitBounds(poly.getBounds());
        }).addTo(map);

        var allData = { total: null, daily: {}, current: null };
        var currentLayer = null;
        var lastReadingTime = "', file = html_file, sep = "")

cat(most_recent_datetime, file = html_file, sep = "")

cat('";
        var dataLoaded = { total: false, current: false };
        var hexDataVisible = true;
        var previousView = "all";

        // If the user clicks the welcome start button before data is loaded,
        // we keep the modal open with a spinner and close it automatically
        // once both the total and current datasets have arrived.
        var waitingToStart = false;
        function isDataReady() { return dataLoaded.total && dataLoaded.current; }
        function maybeFinishStart() {
            if (waitingToStart && isDataReady()) {
                waitingToStart = false;
                closeWelcomeModal();
            }
        }
        function startFromWelcome() {
            if (isDataReady()) { closeWelcomeModal(); return; }
            var btn = document.getElementById("welcomeStartBtn");
            if (btn && !waitingToStart) {
                waitingToStart = true;
                btn.disabled = true;
                btn.innerHTML = \'<span class="btn-spinner"></span>Chargement&hellip;\';
            }
        }

        function toggleHexData() {
            var btn = document.getElementById("toggleDataBtn");
            var dateSelect = document.getElementById("dateSelect");

            if (hexDataVisible) {
                // Hide data
                if (currentLayer) {
                    map.removeLayer(currentLayer);
                }
                previousView = dateSelect.value;
                btn.textContent = "Afficher les données";
                hexDataVisible = false;
            } else {
                // Show data
                dateSelect.value = previousView;
                updateMap();
                btn.textContent = "Masquer les données";
                hexDataVisible = true;
            }
        }

        // Load current outages FIRST (priority)
        fetch("current.geojson")
            .then(function(r) { 
                if (!r.ok) throw new Error("HTTP " + r.status);
                return r.json(); 
            })
            .then(function(data) {
                console.log("Current outages loaded:", data.features ? data.features.length : 0, "features");
                allData.current = data;
                dataLoaded.current = true;
                updateCurrentCount();
                maybeFinishStart();
                // If current is selected, update map
                var dateSelect = document.getElementById("dateSelect");
                if (dateSelect && dateSelect.value === "current") {
                    updateMap();
                }
            })
            .catch(function(e) {
                console.error("Error loading current.geojson:", e);
                allData.current = { type: "FeatureCollection", features: [] };
                dataLoaded.current = true;
                updateCurrentCount();
                maybeFinishStart();
            });
        
        // Load regional data files and merge them
        var regions = ["west", "central_west", "central", "central_east", "east", "far_east"];
        var regionalPromises = regions.map(function(region) {
            return fetch("total/total_exposure_" + region + ".geojson").then(function(r) { return r.json(); });
        });

        Promise.all(regionalPromises)
            .then(function(regionalData) {
                // Merge all regions
                var allFeatures = [];
                regionalData.forEach(function(data) {
                    if (data.features) {
                        allFeatures = allFeatures.concat(data.features);
                    }
                });
                allData.total = { type: "FeatureCollection", features: allFeatures };
                console.log("Total data loaded from", regions.length, "regions:", allFeatures.length, "features");
                // Compute quartile breaks from overall total_occurrences distribution
                quartileBreaks = computeQuartiles(allData.total.features);
                console.log("Quartile breaks:", quartileBreaks);
                rebuildLegend();
                // Update stats panel totals
                var totalOcc = 0;
                allData.total.features.forEach(function(f) {
                    var v = f.properties.total_occurrences;
                    if (typeof v === "number" && !isNaN(v)) totalOcc += v;
                });
                var totalOccSpan = document.getElementById("totalOccurrences");
                if (totalOccSpan) totalOccSpan.innerHTML = "<span class=\\"info-value\\">" + totalOcc.toLocaleString("fr-CA") + "</span>";
                var welcomeCountEl = document.getElementById("welcomeCount");
                if (welcomeCountEl) welcomeCountEl.textContent = totalOcc.toLocaleString("fr-CA");
                var hexCountSpan = document.getElementById("totalHexCount");
                if (hexCountSpan) hexCountSpan.innerHTML = "<span class=\\"info-value\\">" + allData.total.features.length.toLocaleString("fr-CA") + "</span>";
                dataLoaded.total = true;
                maybeFinishStart();
                updateMap();
            })
            .catch(function(e) {
                console.error("Error loading regional data:", e);
                // Re-enable the welcome start button so the user can dismiss.
                if (waitingToStart) {
                    waitingToStart = false;
                    var btn = document.getElementById("welcomeStartBtn");
                    if (btn) {
                        btn.disabled = false;
                        btn.textContent = "Accéder à l\'outil";
                    }
                }
            });
        
        var dates = ', file = html_file, sep = "")

cat(dates_json, file = html_file, sep = "")

cat(';
        
        function loadDailyData() {
            dates.forEach(function(date) {
                fetch("daily/daily_" + date + ".geojson")
                    .then(function(r) { return r.json(); })
                    .then(function(data) { allData.daily[date] = data; })
                    .catch(function(e) {});
            });
        }
        loadDailyData();

        // Auto-refresh: Reload data every 5 minutes to get latest updates
        function refreshData() {
            console.log("Auto-refreshing data...");

            // Reload current outages
            fetch("current.geojson?" + Date.now())
                .then(function(r) {
                    if (!r.ok) throw new Error("HTTP " + r.status);
                    return r.json();
                })
                .then(function(data) {
                    allData.current = data;
                    updateCurrentCount();
                    var dateSelect = document.getElementById("dateSelect");
                    if (dateSelect && dateSelect.value === "current") {
                        updateMap();
                    }

                    // Show brief update notification
                    showUpdateNotification();
                })
                .catch(function(e) {
                    console.error("Error refreshing current.geojson:", e);
                });
        }

        function showUpdateNotification() {
            var notification = document.createElement("div");
            notification.style.cssText = "position: fixed; top: 20px; right: 20px; background: #10b981; color: white; padding: 12px 20px; border-radius: 6px; font-size: 14px; z-index: 10000; box-shadow: 0 4px 6px rgba(0,0,0,0.1);";
            notification.textContent = "✓ Données mises à jour";
            document.body.appendChild(notification);

            setTimeout(function() {
                notification.style.opacity = "0";
                notification.style.transition = "opacity 0.5s";
                setTimeout(function() { document.body.removeChild(notification); }, 500);
            }, 2000);
        }

        // Refresh every 5 minutes (300000 ms)
        setInterval(refreshData, 300000);

        function updateCurrentCount() {
            var countSpan = document.getElementById("currentCount");
            if (countSpan) {
                if (allData.current && allData.current.features) {
                    var count = allData.current.features.length;
                    if (count > 0) {
                        countSpan.innerHTML = "<span class=\\"current-badge\\">" + count + " hexagone" + (count !== 1 ? "s" : "") + "</span>";
                    } else {
                        countSpan.innerHTML = "<span class=\\"info-value\\">0 hexagones</span>";
                    }
                } else {
                    countSpan.innerHTML = "<span class=\\"info-value\\">0 hexagones</span>";
                }
            }
            var timeSpan = document.getElementById("lastReading");
            if (timeSpan) {
                timeSpan.innerHTML = "<span class=\\"info-value\\" style=\\"font-size:12px;\\">" + lastReadingTime + "</span>";
            }
        }
        
        var quartileBreaks = null; // computed from overall total_occurrences on load
        var QUARTILE_COLORS = ["#fee0d2", "#fcbba1", "#fc9272", "#ef3b2c", "#a50f15"];

        function percentile(sortedVals, p) {
            var idx = (sortedVals.length - 1) * p;
            var lo = Math.floor(idx);
            var hi = Math.ceil(idx);
            if (lo === hi) return sortedVals[lo];
            return sortedVals[lo] + (sortedVals[hi] - sortedVals[lo]) * (idx - lo);
        }

        function computeQuartiles(features) {
            var values = [];
            for (var i = 0; i < features.length; i++) {
                var v = features[i].properties.total_occurrences;
                if (typeof v === "number" && !isNaN(v)) values.push(v);
            }
            values.sort(function(a, b) { return a - b; });
            if (values.length === 0) return null;
            return {
                min: values[0],
                q1: Math.round(percentile(values, 0.20)),
                q2: Math.round(percentile(values, 0.40)),
                q3: Math.round(percentile(values, 0.60)),
                q4: Math.round(percentile(values, 0.80)),
                max: values[values.length - 1]
            };
        }

        function getColor(d) {
            if (!quartileBreaks) return "#fcbba1";
            if (d <= quartileBreaks.q1) return QUARTILE_COLORS[0];
            if (d <= quartileBreaks.q2) return QUARTILE_COLORS[1];
            if (d <= quartileBreaks.q3) return QUARTILE_COLORS[2];
            if (d <= quartileBreaks.q4) return QUARTILE_COLORS[3];
            return QUARTILE_COLORS[4];
        }

        function rebuildLegend() {
            var container = document.getElementById("legendItems");
            if (!container || !quartileBreaks) return;
            var q = quartileBreaks;
            var labels = ["Minimal", "Faible", "Modéré", "Élevé", "Intense"];
            var html = "";
            for (var i = 0; i < 5; i++) {
                html += "<div class=\\"legend-item\\"><i style=\\"background:" + QUARTILE_COLORS[i] + "\\"></i> " + labels[i] + "</div>";
            }
            var sentence = "Un hexagone est classé comme ayant un impact minimal lorsqu&apos;il compte entre " +
                q.min + " et " + q.q1 + " occurrences de pannes (mesurées aux 15 minutes), faible entre " +
                (q.q1 + 1) + " et " + q.q2 + ", modéré entre " +
                (q.q2 + 1) + " et " + q.q3 + ", élevé entre " +
                (q.q3 + 1) + " et " + q.q4 + " et intense entre " +
                (q.q4 + 1) + " et " + q.max + ". Chaque niveau regroupe 20% des hexagones. Ces valeurs sont mises à jour automatiquement pour refléter une proportion égale d&apos;observations dans chaque niveau d&apos;impact.";
            html +=
                "<div class=\\"legend-info-wrap\\">" +
                "<div id=\\"legendInfoPopover\\" class=\\"legend-info-popover\\">" + sentence + "</div>" +
                "<button type=\\"button\\" class=\\"legend-info-btn\\" aria-label=\\"Comment sont calculés les seuils\\" onclick=\\"toggleLegendInfo(event)\\">i</button>" +
                "</div>";
            container.innerHTML = html;
        }

        function toggleLegendInfo(evt) {
            if (evt) evt.stopPropagation();
            var pop = document.getElementById("legendInfoPopover");
            if (!pop) return;
            var willOpen = !pop.classList.contains("open");
            pop.classList.toggle("open", willOpen);
            if (willOpen) {
                setTimeout(function() {
                    document.addEventListener("click", function h(e) {
                        if (!e.target.closest(".legend-info-wrap")) {
                            pop.classList.remove("open");
                            document.removeEventListener("click", h);
                        }
                    });
                }, 0);
            }
        }
        
        function updateMap() {
            var dateSelect = document.getElementById("dateSelect");
            var dateFilter = dateSelect ? dateSelect.value : "all";
            var data = null;
            var isCurrent = false;
            
            console.log("updateMap called with filter:", dateFilter);
            
            if (dateFilter === "current") {
                data = allData.current;
                isCurrent = true;
                if (!data || !data.features) {
                    console.log("Current data not loaded yet, waiting...");
                    if (allData.total) {
                        data = allData.total;
                        isCurrent = false;
                    } else {
                        return;
                    }
                }
            } else if (dateFilter === "all") {
                data = allData.total;
            } else if (allData.daily[dateFilter]) {
                data = allData.daily[dateFilter];
            } else {
                data = allData.total;
            }
            
            if (!data || !data.features) {
                console.log("No data available for filter:", dateFilter);
                return;
            }
            
            console.log("Displaying", data.features.length, "features, isCurrent:", isCurrent);
            
            if (currentLayer) map.removeLayer(currentLayer);
            
            if (data.features.length === 0) {
                console.log("No features to display");
                return;
            }
            
            currentLayer = L.geoJSON(data, {
                style: function(f) {
                    var count = f.properties.total_occurrences || f.properties.occurrences_today || 1;
                    return {
                        fillColor: isCurrent ? "#fde68a" : getColor(count),
                        weight: isCurrent ? 2.5 : 0.5,
                        color: isCurrent ? "#b45309" : "#fff",
                        fillOpacity: isCurrent ? 0.75 : 0.75
                    };
                },
                onEachFeature: function(f, layer) {
                    var props = f.properties;
                    var count = props.total_occurrences || props.occurrences_today || 1;
                    var popupContent = "<b>Hexagone #" + props.hex_id + "</b>";
                    if (isCurrent) {
                        popupContent += " <span class=\\"current-badge\\">EN COURS</span>";
                    }
                    popupContent += "<br><b>Occurrences totales:</b> " + count + "<br>" +
                        "<b>Jours affectés:</b> " + (props.days_affected || 1) + "<br>" +
                        "<b>Centroïde:</b> " + props.centroid_lat.toFixed(6) + ", " + props.centroid_lon.toFixed(6) + "<br>" +
                        "<a class=\\"detail-link\\" onclick=\\"showDetails(" + props.hex_id + ")\\">Voir historique</a>";
                    layer.bindPopup(popupContent);
                }
            }).addTo(map);
        }
        
        function showDetails(hexId) {
            var feature = null;
            if (allData.total && allData.total.features) {
                for (var i = 0; i < allData.total.features.length; i++) {
                    if (allData.total.features[i].properties.hex_id === hexId) {
                        feature = allData.total.features[i];
                        break;
                    }
                }
            }
            if (!feature) {
                alert("Historique non disponible pour cet hexagone");
                return;
            }
            
            var props = feature.properties;
            var allDatetimes = props.all_datetimes || "";
            var datetimes = allDatetimes.split(",").map(function(s) { return s.trim(); }).filter(function(s) { return s.length > 0; }).sort();
            
            var byDate = {};
            datetimes.forEach(function(dt) {
                var parts = dt.split(" ");
                var date = parts[0];
                var time = parts[1] || "";
                if (!byDate[date]) byDate[date] = [];
                byDate[date].push(time);
            });
            
            var uniqueDates = Object.keys(byDate).sort().reverse();

            var html = "<h3>Hexagone #" + hexId + "</h3>";

            var isCurrentlyAffected = false;
            if (allData.current && allData.current.features) {
                for (var c = 0; c < allData.current.features.length; c++) {
                    if (allData.current.features[c].properties.hex_id === hexId) {
                        isCurrentlyAffected = true;
                        break;
                    }
                }
            }
            if (isCurrentlyAffected) {
                html += "<p><span class=\\"current-badge\\">PANNE EN COURS</span></p>";
            }

            var totalMin = props.total_occurrences * 15;
            var h = Math.floor(totalMin / 60);
            var m = totalMin % 60;
            var totalTime;
            if (h === 0) {
                totalTime = m + " min";
            } else {
                totalTime = h + " " + (h === 1 ? "heure" : "heures");
                if (m > 0) totalTime += " " + m + " min";
            }
            html += "<p><strong>Nombre d\'occurrences de 15 min.:</strong> " + props.total_occurrences + "</p>";
            html += "<p><strong>Temps total de pannes:</strong> " + totalTime + "</p>";
            html += "<p><strong>Jours affectés:</strong> " + uniqueDates.length + "</p>";
            html += "<p style=\\"margin-top: 15px;\\"><strong>Historique :</strong></p>";
            html += "<div style=\\"max-height: 400px; overflow-y: auto;\\">";

            for (var j = 0; j < uniqueDates.length; j++) {
                var dateKey = uniqueDates[j];
                var times = byDate[dateKey];
                html += "<div class=\\"date-row\\">";
                html += "<div class=\\"date-header\\" onclick=\\"toggleTimes(this)\\">";
                html += "&#9654; " + dateKey + " (" + times.length + " occurrence" + (times.length > 1 ? "s" : "") + ")";
                html += "</div>";
                html += "<div class=\\"times-list\\">";
                for (var k = 0; k < times.length; k++) {
                    html += "<div>" + times[k] + "</div>";
                }
                html += "</div></div>";
            }

            html += "</div>";

            document.getElementById("modalContent").innerHTML = html;
            document.getElementById("detailModal").classList.add("active");
            document.getElementById("modalOverlay").classList.add("active");
        }

        function toggleTimes(header) {
            var timesList = header.nextElementSibling;
            if (timesList.style.display === "block") {
                timesList.style.display = "none";
            } else {
                timesList.style.display = "block";
            }
        }
        
        function closeDetailModal() {
            document.getElementById("detailModal").classList.remove("active");
            document.getElementById("modalOverlay").classList.remove("active");
        }
        // Tracks whether the FAQ modal was opened from the welcome modal, so
        // closing the FAQ (via ×, overlay click, Esc) returns the user to the
        // welcome modal instead of dismissing everything and starting the app.
        var faqOpenedFromWelcome = false;
        function openFaqModal() {
            document.getElementById("faqModal").classList.add("active");
            document.getElementById("modalOverlay").classList.add("active");
        }
        function closeFaqModal() {
            document.getElementById("faqModal").classList.remove("active");
            if (faqOpenedFromWelcome) {
                faqOpenedFromWelcome = false;
                document.getElementById("welcomeModal").classList.add("active");
                // Leave the overlay active so the welcome modal stays modal.
            } else {
                document.getElementById("modalOverlay").classList.remove("active");
            }
        }
        function closeWelcomeModal() {
            document.getElementById("welcomeModal").classList.remove("active");
            if (!document.getElementById("faqModal").classList.contains("active") &&
                !document.getElementById("detailModal").classList.contains("active")) {
                document.getElementById("modalOverlay").classList.remove("active");
            }
        }
        function openFaqFromWelcome() {
            faqOpenedFromWelcome = true;
            document.getElementById("welcomeModal").classList.remove("active");
            openFaqModal();
        }
        function closeAllModals() {
            // If the FAQ is open on top of the welcome modal, clicking the
            // overlay should only close the FAQ and return the user to the
            // welcome modal — don\'t cascade and dismiss everything.
            if (document.getElementById("faqModal").classList.contains("active")) {
                closeFaqModal();
                return;
            }
            closeDetailModal();
            closeWelcomeModal();
        }

        // Fetch total visitor count from GoatCounter and show the pill.
        // If the request fails, the pill stays hidden.
        (function loadVisitorCount() {
            fetch("https://ebudem.goatcounter.com/counter/TOTAL.json")
                .then(function(r) { return r.ok ? r.json() : null; })
                .then(function(data) {
                    if (!data || !data.count) return;
                    var n = parseInt(String(data.count).replace(/\\s/g, ""), 10);
                    if (isNaN(n) || n <= 0) return;
                    document.getElementById("visitorCount").textContent = n.toLocaleString("fr-CA");
                    document.getElementById("visitorPill").classList.add("visible");
                })
                .catch(function() { /* silent */ });
        })();

        var legend = L.control({position: "bottomright"});
        legend.onAdd = function() {
            var div = L.DomUtil.create("div", "info legend");
            div.id = "legend";
            div.innerHTML = "<h4>Impact relatif des pannes</h4>" +
                "<div id=\\"legendItems\\"><div style=\\"font-size:12px;color:#999;\\">Chargement...</div></div>";
            return div;
        };
        legend.addTo(map);
        if (quartileBreaks) { rebuildLegend(); }
        
        var info = L.control({position: "topright"});
        info.onAdd = function() {
            var div = L.DomUtil.create("div", "info");
            div.innerHTML = "<h4>Statistiques</h4>" +
                "<div class=\\"info-row\\"><span class=\\"info-label\\">Pannes en cours:</span> <span id=\\"currentCount\\" class=\\"loading\\">chargement...</span></div>" +
                "<div class=\\"info-row\\"><span class=\\"info-label\\">Dernière lecture:</span> <span id=\\"lastReading\\" class=\\"loading\\">chargement...</span></div>" +
                "<div class=\\"info-row\\"><span class=\\"info-label\\">Nombre total d&apos;occurrences<br>en 2026:</span> <span id=\\"totalOccurrences\\" class=\\"loading\\">chargement...</span></div>" +
                "<div class=\\"info-row\\"><span class=\\"info-label\\">Nombre d&apos;hexagones impactés<br>en 2026:</span> <span id=\\"totalHexCount\\" class=\\"loading\\">chargement...</span></div>" +
                "<div style=\\"margin-top: 10px; padding-top: 10px; border-top: 1px solid #e5e7eb;font-size:11px;color:#9ca3af;line-height:1.6;\\">" +
                    "<div>Généré le ', file = html_file, sep = "")

cat(current_date_fr, file = html_file, sep = "")

cat('</div>" +
                    "<div>Taille hexagones: ', file = html_file, sep = "")

cat(hex_size_km, file = html_file, sep = "")

cat(' km</div>" +
                    "<div>Jours analysés: ', file = html_file, sep = "")

cat(num_days, file = html_file, sep = "")

cat('</div>" +
                    "<div>1 occurrence: panne de 15 minutes</div>" +
                "</div>";
            return div;
        };
        info.addTo(map);
        
        var controls = L.control({position: "topleft"});
        controls.onAdd = function() {
            var div = L.DomUtil.create("div", "controls");
            var selectHtml = "<h4>Affichage</h4><select id=\\"dateSelect\\" onchange=\\"updateMap()\\">";
            selectHtml += "<option value=\\"all\\" selected>Historique complet</option>";
            selectHtml += "<option value=\\"current\\">Pannes en cours</option>";
            selectHtml += "<optgroup label=\\"Par jour\\">";
            var sortedDates = dates.slice().sort().reverse();
            sortedDates.forEach(function(d) {
                selectHtml += "<option value=\\"" + d + "\\">" + d + "</option>";
            });
            selectHtml += "</optgroup>";
            selectHtml += "</select>";
            selectHtml += "<button id=\\"toggleDataBtn\\" onclick=\\"toggleHexData()\\" class=\\"toggle-data-btn\\">Masquer les données</button>";
            div.innerHTML = selectHtml;
            return div;
        };
        controls.addTo(map);
        
        L.Control.geocoder({
            defaultMarkGeocode: false,
            placeholder: "Rechercher une adresse..."
        }).on("markgeocode", function(e) {
            map.fitBounds(e.geocode.bbox);
        }).addTo(map);
        
        setTimeout(updateCurrentCount, 1000);
    </script>
</body>
</html>', file = html_file, sep = "")

close(html_file)
cat("  ✓ HTML visualization created\n")

# ==================================================================
# STEP 8: CLEANUP OLD SNAPSHOTS
# ==================================================================
cat("\n[8] Managing snapshot history...\n")

all_snapshots <- list.files(cumulative_snapshots_dir, 
                            pattern = "^cumulative_.*\\.geojson$", 
                            full.names = TRUE)

if (length(all_snapshots) > 30) {
  snapshot_info <- file.info(all_snapshots)
  snapshot_info <- snapshot_info[order(snapshot_info$mtime, decreasing = TRUE), ]
  snapshots_to_delete <- rownames(snapshot_info)[31:nrow(snapshot_info)]
  
  for (f in snapshots_to_delete) file.remove(f)
  
  cat(sprintf("  ✓ Cleaned up %d old snapshots (keeping last 30)\n", length(snapshots_to_delete)))
} else {
  cat(sprintf("  ✓ Snapshot history: %d files\n", length(all_snapshots)))
}

# ==================================================================
# FINAL SUMMARY
# ==================================================================
elapsed <- as.numeric(difftime(Sys.time(), start_time, units = "secs"))
total_files <- length(files)

cat("\n========================================\n")
cat("[OK] PROCESSING COMPLETE\n")
cat("========================================\n")
cat(sprintf("Total processing time: %.1f seconds\n", elapsed))
cat(sprintf("Files processed: %d\n", total_files))
cat(sprintf("Unique hexagons affected (all time): %d\n", length(cumulative_hex_data)))
cat(sprintf("Current outages: %d hexagons\n", current_outage_count))
cat(sprintf("Date range: %s to %s\n", min(files_dt$date), max(files_dt$date)))
cat(sprintf("Daily summaries: %d\n", length(unique_dates)))
cat(sprintf("Monthly summaries: %d\n", length(unique_months)))
cat(sprintf("Most recent reading: %s\n", most_recent_datetime))
cat("========================================\n")
cat("Output structure:\n")
cat("  • public/current.geojson (CURRENT OUTAGES)\n")
cat("  • public/total/total_exposure_[region].geojson (6 regional files)\n")
cat("  • public/daily/daily_YYYY-MM-DD.geojson\n")
cat("  • public/monthly/monthly_YYYY-MM.geojson\n")
cat("  • public/index.html\n")
cat("========================================\n")
