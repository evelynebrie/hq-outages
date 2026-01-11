# HQ Outages - 15-MINUTE SCRAPE VERSION with Timestamp Tracking
# Adapted for data scraped every ~15 minutes with exact timestamps in filenames
# Tracks: Each time a hex appears in an outage file, its score increases by 1

suppressPackageStartupMessages({
  library(sf)
  library(dplyr)
  library(jsonlite)
  library(data.table)
})

cat("=== HQ Outages 15-Minute Scrape Analysis System ===\n")
cat("Features:\n")
cat("  • Processes files with exact timestamps (YYYYMMDDTHHMMSS)\n")
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
SIMPLIFY <- 200      # Simplification tolerance

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
cat("\n[1] Scanning and parsing input files...\n")

# Look for files matching new naming pattern
# Expected: outages_joined_full_YYYYMMDDTHHMMSS.geojson OR polygons_YYYYMMDDTHHMMSS.geojson
files <- list.files(data_path, 
                   pattern = "(outages_joined_full_|polygons_).*\\d{8}[Tt]\\d{6}.*\\.geojson$", 
                   full.names = TRUE)

if (length(files) == 0) {
  cat("⚠️  No files found matching pattern. Looking for files like:\n")
  cat("   - outages_joined_full_YYYYMMDDTHHMMSS.geojson\n")
  cat("   - polygons_YYYYMMDDTHHMMSS.geojson\n")
  stop("No input files found")
}

# ==================================================================
# LOAD CACHE IF IT EXISTS
# ==================================================================
cumulative_hex_data <- list()
processed_files <- character()

if (file.exists(cache_file)) {
  cat("\n[CACHE] Loading previously processed data...\n")
  cached_data <- readRDS(cache_file)
  cumulative_hex_data <- cached_data$cumulative_hex_data
  processed_files <- cached_data$processed_files
  cat(sprintf("  ✓ Loaded cache with %d hexagons and %d processed files\n", 
              length(cumulative_hex_data), length(processed_files)))
}

# Filter to only new files
new_files <- setdiff(files, processed_files)

if (length(new_files) == 0) {
  cat("\n✅ All files already processed! Using cached data.\n")
  cat(sprintf("  • Total files: %d\n", length(files)))
  cat(sprintf("  • Already processed: %d\n", length(processed_files)))
  cat(sprintf("  • New files: 0\n"))
  cat("\nSkipping to summaries generation...\n")
  
  # Jump to summaries section (we'll still need to regenerate summaries)
  files_to_process <- character()
} else {
  cat(sprintf("\n📊 File Summary:\n"))
  cat(sprintf("  • Total files found: %d\n", length(files)))
  cat(sprintf("  • Previously processed: %d\n", length(processed_files)))
  cat(sprintf("  • New files to process: %d\n", length(new_files)))
  
  files_to_process <- new_files
}

# Parse timestamps for ALL files (needed for summaries)
files_dt_all <- data.frame(
  file = files,
  stringsAsFactors = FALSE
)

basenames <- basename(files)
cat("  Sample filenames:\n")
cat(paste("   ", head(basenames, 3), collapse = "\n"), "\n")

# Extract timestamp part (YYYYMMDDTHHMMSS)
timestamps <- regmatches(basenames, 
                        regexpr("\\d{8}[Tt]\\d{6}", basenames, ignore.case = TRUE))

if (length(timestamps) == 0) {
  stop("❌ Could not parse timestamps from filenames. Expected format: *_YYYYMMDDTHHMMSS.geojson")
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
    # Read and process polygons
    polys <- st_read(f, quiet = TRUE) %>%
      st_transform(32618) %>%
      st_simplify(dTolerance = SIMPLIFY, preserveTopology = FALSE)
    
    # Union all polygons in this file
    if (nrow(polys) > 1) polys <- st_union(polys)
    
    # Find affected hexagons
    hits <- st_intersects(hex_grid_reference, polys, sparse = TRUE)
    affected_ids <- which(lengths(hits) > 0)
    
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
    
    rm(polys, hits)
    gc(verbose = FALSE)
    
  }, error = function(e) {
    cat(sprintf("    ⚠ Error processing file %s: %s\n", basename(f), e$message))
  })
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

# ==================================================================
# STEP 3.5: GENERATE CURRENT OUTAGES (from most recent file)
# ==================================================================
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
  current_polys <- st_read(most_recent_file, quiet = TRUE) %>%
    st_transform(32618) %>%
    st_simplify(dTolerance = SIMPLIFY, preserveTopology = FALSE)
  
  cat(sprintf("  Read %d polygons from file\n", nrow(current_polys)))
  
  # Union all polygons
  if (nrow(current_polys) > 1) {
    current_polys <- st_union(current_polys)
  }
  
  # Find affected hexagons
  current_hits <- st_intersects(hex_grid_reference, current_polys, sparse = TRUE)
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
    # No current outages - create empty GeoJSON with proper structure
    cat("  Creating empty current.geojson (no active outages)\n")
    
    # Create a valid empty GeoJSON file manually
    empty_geojson <- '{"type":"FeatureCollection","features":[]}'
    writeLines(empty_geojson, file.path(output_dir, "current.geojson"))
    cat("  ✓ Empty current.geojson created\n")
  }
  
  rm(current_polys, current_hits)
  gc(verbose = FALSE)
  
}, error = function(e) {
  cat(sprintf("  ⚠ Error processing current outages: %s\n", e$message))
  # Create empty file on error
  empty_geojson <- '{"type":"FeatureCollection","features":[]}'
  writeLines(empty_geojson, file.path(output_dir, "current.geojson"))
})

# ==================================================================
# STEP 4: GENERATE DAILY SUMMARIES (OPTIMIZED - USE EXISTING DATA)
# ==================================================================
cat("\n[4] Generating daily summaries...\n")

unique_dates <- unique(files_dt$date)
cat(sprintf("  Processing %d unique dates...\n", length(unique_dates)))

# Build daily summaries from cumulative data (much faster!)
for (date in unique_dates) {
  cat(sprintf("  • %s...", date))
  
  # Aggregate hex data for this date from cumulative data
  daily_hex_data <- list()
  
  for (hex_key in names(cumulative_hex_data)) {
    hex_info <- cumulative_hex_data[[hex_key]]
    
    # Find which datetimes belong to this date
    date_mask <- hex_info$dates == date
    
    if (any(date_mask)) {
      daily_hex_data[[hex_key]] <- list(
        count = sum(date_mask),
        datetimes = hex_info$datetimes[date_mask]
      )
    }
  }
  
  # Create daily summary GeoJSON
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
    
    # Transform to WGS84 and save
    daily_output <- daily_summary %>%
      st_transform(4326)
    
    output_file <- file.path(output_dir, "daily", sprintf("daily_%s.geojson", date))
    st_write(daily_output, output_file, delete_dsn = TRUE, quiet = TRUE)
    cat(" ✓\n")
  } else {
    cat(" (no data)\n")
  }
}

cat(sprintf("  ✓ Generated daily summaries for %d dates\n", length(unique_dates)))

# ==================================================================
# STEP 5: GENERATE MONTHLY SUMMARIES (OPTIMIZED)
# ==================================================================
cat("\n[5] Generating monthly summaries...\n")

unique_months <- unique(files_dt$yearmon)
cat(sprintf("  Processing %d unique months...\n", length(unique_months)))

# Build monthly summaries from cumulative data (much faster!)
for (month in unique_months) {
  cat(sprintf("  • %s...", month))
  
  # Get dates in this month
  month_dates <- unique(files_dt$date[files_dt$yearmon == month])
  
  monthly_hex_data <- list()
  
  for (hex_key in names(cumulative_hex_data)) {
    hex_info <- cumulative_hex_data[[hex_key]]
    
    # Find which datetimes belong to this month
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
  
  # Create monthly summary
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

cat(sprintf("  ✓ Generated monthly summaries for %d months\n", length(unique_months)))

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

output_file <- file.path(output_dir, "total", "total_exposure.geojson")
st_write(total_output, output_file, delete_dsn = TRUE, quiet = TRUE)

cat(sprintf("  ✓ Total hexagons affected: %d\n", nrow(total_output)))
cat(sprintf("  ✓ Max occurrences per hex: %d\n", max(total_summary$total_occurrences)))
cat(sprintf("  ✓ Mean occurrences per hex: %.1f\n", mean(total_summary$total_occurrences)))

# Save cumulative snapshot
snapshot_date <- format(Sys.Date(), "%Y%m%d")
snapshot_file <- file.path(cumulative_snapshots_dir, 
                          sprintf("cumulative_%s.geojson", snapshot_date))
st_write(total_output, snapshot_file, delete_dsn = TRUE, quiet = TRUE)
cat(sprintf("  ✓ Snapshot saved: cumulative_%s.geojson\n", snapshot_date))

# ==================================================================
# STEP 7: GENERATE HTML VISUALIZATION
# ==================================================================
cat("\n[7] Generating HTML visualization...\n")

# Calculate statistics
num_days <- length(unique(files_dt$date))
hex_size_km <- sprintf("%.1f", HEX_SIZE / 1000)
current_date <- format(Sys.time(), "%Y-%m-%d %H:%M")

# Build dates JSON array
dates_json <- toJSON(unique_dates)

# Write HTML file directly using cat() to avoid any escaping issues
html_file <- file(file.path(output_dir, "index.html"), "w")

cat('<!DOCTYPE html>
<html lang="fr">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>HQ Outages - Pannes Hydro-Quebec</title>
    <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
    <link rel="stylesheet" href="https://unpkg.com/leaflet-control-geocoder@2.4.0/dist/Control.Geocoder.css" />
    <style>
        body { margin: 0; font-family: Arial, sans-serif; }
        #map { height: 100vh; width: 100%; }
        .info { padding: 6px 8px; background: white; border-radius: 5px; box-shadow: 0 0 15px rgba(0,0,0,0.2); }
        .info h4 { margin: 0 0 5px; color: #333; }
        .legend { line-height: 18px; color: #555; }
        .legend i { width: 18px; height: 18px; float: left; margin-right: 8px; opacity: 0.7; }
        .controls { padding: 10px; background: white; border-radius: 5px; box-shadow: 0 0 15px rgba(0,0,0,0.2); }
        .controls select { width: 100%; margin: 5px 0; padding: 5px; }
        .controls label { font-weight: bold; display: block; margin-top: 8px; }
        .detail-link { color: #0078A8; cursor: pointer; text-decoration: underline; }
        #detailModal { position: fixed; top: 50%; left: 50%; transform: translate(-50%, -50%); 
                      background: white; padding: 20px; border-radius: 10px; box-shadow: 0 0 30px rgba(0,0,0,0.3);
                      max-height: 80vh; overflow-y: auto; z-index: 10000; display: none; min-width: 300px; }
        #detailModal.active { display: block; }
        #modalOverlay { position: fixed; top: 0; left: 0; width: 100%; height: 100%; 
                       background: rgba(0,0,0,0.5); z-index: 9999; display: none; }
        #modalOverlay.active { display: block; }
        .date-row { margin: 5px 0; border-bottom: 1px solid #eee; padding: 5px 0; }
        .date-header { cursor: pointer; font-weight: bold; color: #0078A8; }
        .times-list { display: none; margin-left: 20px; margin-top: 5px; font-size: 0.9em; color: #666; }
        .current-badge { background: #e74c3c; color: white; padding: 2px 8px; border-radius: 3px; font-size: 12px; margin-left: 5px; }
        .current-option { font-weight: bold; color: #e74c3c; }
        .loading { color: #999; font-style: italic; }
    </style>
</head>
<body>
    <div id="map"></div>
    <div id="modalOverlay" onclick="closeDetailModal()"></div>
    <div id="detailModal">
        <button onclick="closeDetailModal()" style="float: right; border: none; background: none; font-size: 18px; cursor: pointer;">X</button>
        <div id="modalContent"></div>
    </div>
    
    <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
    <script src="https://unpkg.com/leaflet-control-geocoder@2.4.0/dist/Control.Geocoder.js"></script>
    <script>
        var map = L.map("map").setView([46.8, -71.2], 7);
        L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
            attribution: "OpenStreetMap"
        }).addTo(map);
        
        var allData = { total: null, daily: {}, current: null };
        var currentLayer = null;
        var lastReadingTime = "', file = html_file, sep = "")

cat(most_recent_datetime, file = html_file, sep = "")

cat('";
        var dataLoaded = { total: false, current: false };
        
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
            });
        
        // Load total data
        fetch("total/total_exposure.geojson")
            .then(function(r) { return r.json(); })
            .then(function(data) {
                console.log("Total data loaded:", data.features ? data.features.length : 0, "features");
                allData.total = data;
                dataLoaded.total = true;
                // Initial map update - show current if loaded, otherwise total
                updateMap();
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
        
        function updateCurrentCount() {
            var countSpan = document.getElementById("currentCount");
            if (countSpan) {
                if (allData.current && allData.current.features) {
                    var count = allData.current.features.length;
                    countSpan.textContent = count + " hexagone" + (count !== 1 ? "s" : "");
                    countSpan.className = count > 0 ? "current-badge" : "";
                } else {
                    countSpan.textContent = "0 hexagones";
                    countSpan.className = "";
                }
            }
            var timeSpan = document.getElementById("lastReading");
            if (timeSpan) {
                timeSpan.textContent = lastReadingTime;
            }
        }
        
        function getColor(d) {
            if (d > 80) return "#67000d";
            if (d > 60) return "#a50f15";
            if (d > 40) return "#cb181d";
            if (d > 20) return "#ef3b2c";
            if (d > 10) return "#fb6a4a";
            if (d > 5) return "#fc9272";
            if (d > 2) return "#fcbba1";
            return "#fee5d9";
        }
        
        function updateMap() {
            var dateSelect = document.getElementById("dateSelect");
            var dateFilter = dateSelect ? dateSelect.value : "current";
            var data = null;
            var isCurrent = false;
            
            console.log("updateMap called with filter:", dateFilter);
            
            if (dateFilter === "current") {
                data = allData.current;
                isCurrent = true;
                if (!data || !data.features) {
                    console.log("Current data not loaded yet, waiting...");
                    // Show loading state or fall back to total
                    if (allData.total) {
                        data = allData.total;
                        isCurrent = false;
                    } else {
                        return; // Nothing to show yet
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
                // No features to display
                console.log("No features to display");
                return;
            }
            
            currentLayer = L.geoJSON(data, {
                style: function(f) {
                    var count = f.properties.total_occurrences || f.properties.occurrences_today || 1;
                    return {
                        fillColor: isCurrent ? "#e74c3c" : getColor(count),
                        weight: isCurrent ? 2 : 0.5,
                        color: isCurrent ? "#c0392b" : "#fff",
                        fillOpacity: isCurrent ? 0.8 : 0.75
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
                        "<b>Jours affectes:</b> " + (props.days_affected || 1) + "<br>" +
                        "<b>Centroide:</b> " + props.centroid_lat.toFixed(6) + ", " + props.centroid_lon.toFixed(6) + "<br>" +
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
            
            var uniqueDates = Object.keys(byDate).sort().reverse(); // Most recent first
            
            var html = "<h3>Hexagone #" + hexId + "</h3>";
            
            // Check if this hex is currently affected
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
            
            html += "<p><strong>Total occurrences:</strong> " + props.total_occurrences + "</p>";
            html += "<p><strong>Jours affectes:</strong> " + uniqueDates.length + "</p>";
            html += "<p style=\\"margin-top: 15px;\\"><strong>Historique (recent en premier):</strong></p>";
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
        
        var legend = L.control({position: "bottomright"});
        legend.onAdd = function() {
            var div = L.DomUtil.create("div", "info legend");
            var grades = [0, 2, 5, 10, 20, 40, 60, 80];
            div.innerHTML = "<h4>Occurrences</h4>";
            div.innerHTML += "<i style=\\"background:#e74c3c\\"></i>En cours<br>";
            for (var i = 0; i < grades.length; i++) {
                div.innerHTML += "<i style=\\"background:" + getColor(grades[i] + 1) + "\\"></i>" +
                    grades[i] + (grades[i + 1] ? "-" + grades[i + 1] : "+") + "<br>";
            }
            return div;
        };
        legend.addTo(map);
        
        var info = L.control({position: "topright"});
        info.onAdd = function() {
            var div = L.DomUtil.create("div", "info");
            div.innerHTML = "<h4>Pannes Hydro-Quebec</h4>" +
                "<p><b>Pannes en cours:</b> <span id=\\"currentCount\\" class=\\"loading\\">chargement...</span></p>" +
                "<p><b>Derniere lecture:</b><br><span id=\\"lastReading\\" class=\\"loading\\">chargement...</span></p>" +
                "<hr style=\\"margin: 8px 0;\\">" +
                "<p style=\\"font-size:11px;\\"><b>Hexagones:</b> ', file = html_file, sep = "")

cat(hex_size_km, file = html_file, sep = "")

cat(' km</p>" +
                "<p style=\\"font-size:11px;\\"><b>Jours analyses:</b> ', file = html_file, sep = "")

cat(num_days, file = html_file, sep = "")

cat('</p>" +
                "<p style=\\"font-size:11px;color:#999;\\">Genere: ', file = html_file, sep = "")

cat(current_date, file = html_file, sep = "")

cat('</p>";
            return div;
        };
        info.addTo(map);
        
        var controls = L.control({position: "topleft"});
        controls.onAdd = function() {
            var div = L.DomUtil.create("div", "controls");
            var selectHtml = "<h4>Affichage</h4><select id=\\"dateSelect\\" onchange=\\"updateMap()\\">";
            selectHtml += "<option value=\\"current\\" selected>&#128308; Pannes en cours</option>";
            selectHtml += "<option value=\\"all\\">Historique complet</option>";
            selectHtml += "<optgroup label=\\"Par jour\\">";
            // Show dates in reverse order (most recent first)
            var sortedDates = dates.slice().sort().reverse();
            sortedDates.forEach(function(d) {
                selectHtml += "<option value=\\"" + d + "\\">" + d + "</option>";
            });
            selectHtml += "</optgroup>";
            selectHtml += "</select>";
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
        
        // Update current count once data is loaded
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
cat("✅ PROCESSING COMPLETE\n")
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
cat("  • public/total/total_exposure.geojson\n")
cat("  • public/daily/daily_YYYY-MM-DD.geojson\n")
cat("  • public/monthly/monthly_YYYY-MM.geojson\n")
cat("  • public/cumulative_snapshots/cumulative_YYYYMMDD.geojson\n")
cat("  • public/index.html\n")
cat("========================================\n")
