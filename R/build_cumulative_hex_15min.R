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

# Create HTML with embedded daily data for filtering
html_content <- sprintf('<!DOCTYPE html>
<html lang="fr">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>HQ Outages - Cumulative Analysis</title>
    <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
    <link rel="stylesheet" href="https://unpkg.com/leaflet-control-geocoder@2.4.0/dist/Control.Geocoder.css" />
    <style>
        body { margin: 0; font-family: Arial, sans-serif; }
        #map { height: 100vh; width: 100%%; }
        .info { padding: 6px 8px; background: white; border-radius: 5px; box-shadow: 0 0 15px rgba(0,0,0,0.2); }
        .info h4 { margin: 0 0 5px; color: #333; }
        .legend { line-height: 18px; color: #555; }
        .legend i { width: 18px; height: 18px; float: left; margin-right: 8px; opacity: 0.7; }
        .controls { padding: 10px; background: white; border-radius: 5px; box-shadow: 0 0 15px rgba(0,0,0,0.2); }
        .controls select { width: 100%%; margin: 5px 0; padding: 5px; }
        .detail-link { color: #0078A8; cursor: pointer; text-decoration: underline; }
        #detailModal { position: fixed; top: 50%%; left: 50%%; transform: translate(-50%%, -50%%); 
                      background: white; padding: 20px; border-radius: 10px; box-shadow: 0 0 30px rgba(0,0,0,0.3);
                      max-height: 80vh; overflow-y: auto; z-index: 10000; display: none; }
        #detailModal.active { display: block; }
        #modalOverlay { position: fixed; top: 0; left: 0; width: 100%%; height: 100%%; 
                       background: rgba(0,0,0,0.5); z-index: 9999; display: none; }
        #modalOverlay.active { display: block; }
        .detail-table { border-collapse: collapse; width: 100%%; margin-top: 10px; }
        .detail-table th, .detail-table td { border: 1px solid #ddd; padding: 8px; text-align: left; }
        .detail-table th { background-color: #f2f2f2; }
    </style>
</head>
<body>
    <div id="map"></div>
    <div id="modalOverlay"></div>
    <div id="detailModal">
        <button onclick="closeDetailModal()" style="float: right;">✕</button>
        <div id="modalContent"></div>
    </div>
    
    <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
    <script src="https://unpkg.com/leaflet-control-geocoder@2.4.0/dist/Control.Geocoder.js"></script>
    <script>
        const map = L.map("map").setView([46.8, -71.2], 7);
        L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
            attribution: "© OpenStreetMap"
        }).addTo(map);
        
        let allData = { total: null, daily: {} };
        let currentLayer = null;
        
        // Load total data
        fetch("total/total_exposure.geojson")
            .then(r => r.json())
            .then(data => {
                allData.total = data;
                updateMap();
            });
        
        // Load daily summaries
        async function loadDailyData() {
            const dates = %s;
            for (const dateStr of dates) {
                try {
                    const response = await fetch("daily/daily_" + dateStr + ".geojson");
                    allData.daily[dateStr] = await response.json();
                } catch (e) {}
            }
        }
        loadDailyData();
        
        function getColor(d) {
            return d > 80 ? "#67000d" : d > 60 ? "#a50f15" : d > 40 ? "#cb181d" :
                   d > 20 ? "#ef3b2c" : d > 10 ? "#fb6a4a" : d > 5 ? "#fc9272" :
                   d > 2 ? "#fcbba1" : "#fee5d9";
        }
        
        function updateMap() {
            const dateFilter = document.getElementById("dateSelect")?.value || "all";
            let data = allData.total;
            
            if (dateFilter !== "all" && allData.daily[dateFilter]) {
                data = allData.daily[dateFilter];
            }
            
            if (!data) return;
            
            if (currentLayer) map.removeLayer(currentLayer);
            
            currentLayer = L.geoJSON(data, {
                style: f => ({
                    fillColor: getColor(f.properties.total_occurrences || f.properties.occurrences_today || 0),
                    weight: 0.5,
                    color: "#fff",
                    fillOpacity: 0.75
                }),
                onEachFeature: (f, layer) => {
                    const props = f.properties;
                    const count = props.total_occurrences || props.occurrences_today || 0;
                    layer.bindPopup(
                        "<b>Hexagone #" + props.hex_id + "</b><br>" +
                        "<b>Occurrences:</b> " + count + "<br>" +
                        "<b>Jours affectés:</b> " + (props.days_affected || 1) + "<br>" +
                        "<b>Centroïde:</b> " + props.centroid_lat.toFixed(6) + ", " + props.centroid_lon.toFixed(6) + "<br>" +
                        "<a class=\\"detail-link\\" onclick=\\"showDetails(" + props.hex_id + ")\\">Voir détails</a>"
                    );
                }
            }).addTo(map);
        }
        
        function showDetails(hexId) {
            const feature = allData.total.features.find(f => f.properties.hex_id === hexId);
            if (!feature) return;
            
            const props = feature.properties;
            const datetimes = (props.all_datetimes || "").split(",").map(s => s.trim()).sort();
            
            // Group by date
            const byDate = {};
            datetimes.forEach(dt => {
                const parts = dt.split(' ');
                if (parts.length >= 2) {
                    const dateKey = parts[0];
                    const timeVal = parts[1];
                    if (!byDate[dateKey]) byDate[dateKey] = [];
                    byDate[dateKey].push(timeVal);
                }
            });
            
            const uniqueDates = Object.keys(byDate).sort();
            
            let html = "<h3>Hexagone #" + hexId + "</h3>";
            html += "<p><strong>Total occurrences:</strong> " + props.total_occurrences + "</p>";
            html += "<p><strong>Total number of affected days:</strong> " + uniqueDates.length + "</p>";
            html += "<p style=\\"margin-top: 15px;\\"><strong>Affected days:</strong></p>";
            html += "<div style=\\"max-height: 400px; overflow-y: auto;\\">";
            
            uniqueDates.forEach(dateKey => {
                const times = byDate[dateKey];
                const dateId = "date_" + hexId + "_" + dateKey.replace(/-/g, "");
                html += "<div style=\\"margin: 5px 0; border-bottom: 1px solid #eee; padding: 5px 0;\\">";
                html += "<div style=\\"cursor: pointer; font-weight: bold; color: #0078A8;\\" onclick=\\"toggleTimes('" + dateId + "')\\">"; 
                html += "▶ " + dateKey + " (" + times.length + " occurrence" + (times.length > 1 ? "s" : "") + ")";
                html += "</div>";
                html += "<div id=\\"" + dateId + "\\" style=\\"display: none; margin-left: 20px; margin-top: 5px; font-size: 0.9em; color: #666;\\">";
                times.forEach(time => {
                    html += "<div>" + time + "</div>";
                });
                html += "</div></div>";
            });
            
            html += "</div>";
            
            document.getElementById("modalContent").innerHTML = html;
            document.getElementById("detailModal").classList.add("active");
            document.getElementById("modalOverlay").classList.add("active");
        }
        
        function toggleTimes(dateId) {
            const elem = document.getElementById(dateId);
            const arrow = event.target.querySelector('span') || event.target;
            if (elem.style.display === 'none') {
                elem.style.display = 'block';
                if (arrow.textContent && arrow.textContent.includes('▶')) {
                    arrow.textContent = arrow.textContent.replace('▶', '▼');
                } else {
                    event.target.innerHTML = event.target.innerHTML.replace('▶', '▼');
                }
            } else {
                elem.style.display = 'none';
                if (arrow.textContent && arrow.textContent.includes('▼')) {
                    arrow.textContent = arrow.textContent.replace('▼', '▶');
                } else {
                    event.target.innerHTML = event.target.innerHTML.replace('▼', '▶');
                }
            }
        }
        
        function closeDetailModal() {
            document.getElementById("detailModal").classList.remove("active");
            document.getElementById("modalOverlay").classList.remove("active");
        }
        
        document.getElementById("modalOverlay").onclick = closeDetailModal;
        
        // Add controls
        const legend = L.control({position: "bottomright"});
        legend.onAdd = () => {
            const div = L.DomUtil.create("div", "info legend");
            div.innerHTML = "<h4>Occurrences</h4>";
            [0, 2, 5, 10, 20, 40, 60, 80].forEach((g, i, a) => {
                div.innerHTML += "<i style=\\"background:" + getColor(g + 1) + "\\"></i>" + g + (a[i + 1] ? "–" + a[i + 1] : "+") + "<br>";
            });
            return div;
        };
        legend.addTo(map);
        
        const info = L.control({position: "topright"});
        info.onAdd = () => {
            const div = L.DomUtil.create("div", "info");
            div.innerHTML = `
                <h4>Pannes de courant cumulatives</h4>
                <p><b>Taille des hexagones:</b> %s km</p>
                <p><b>Jours analysés:</b> %d</p>
                <p style="font-size:11px;color:#999;">Mise à jour: %s</p>
            `;
            return div;
        };
        info.addTo(map);
        
        const controls = L.control({position: "topleft"});
        controls.onAdd = () => {
            const div = L.DomUtil.create("div", "controls");
            div.innerHTML = `
                <h4>Filtres</h4>
                <label>Date:</label>
                <select id="dateSelect" onchange="updateMap()">
                    <option value="all">Toutes (cumulatif)</option>
                    %s
                </select>
            `;
            return div;
        };
        controls.addTo(map);
        
        L.Control.geocoder({
            defaultMarkGeocode: false,
            placeholder: "Rechercher une adresse..."
        }).on("markgeocode", function(e) {
            map.fitBounds(e.geocode.bbox);
        }).addTo(map);
    </script>
</body>
</html>',
  toJSON(unique_dates),
  hex_size_km,
  num_days,
  current_date,
  paste(sprintf('<option value="%s">%s</option>', unique_dates, unique_dates), collapse = "\n")
)

writeLines(html_content, file.path(output_dir, "index.html"))
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

cat("\n========================================\n")
cat("✅ PROCESSING COMPLETE\n")
cat("========================================\n")
cat(sprintf("Total processing time: %.1f seconds\n", elapsed))
cat(sprintf("Files processed: %d\n", total_files))
cat(sprintf("Unique hexagons affected: %d\n", length(cumulative_hex_data)))
cat(sprintf("Date range: %s to %s\n", min(files_dt$date), max(files_dt$date)))
cat(sprintf("Daily summaries: %d\n", length(unique_dates)))
cat(sprintf("Monthly summaries: %d\n", length(unique_months)))
cat("========================================\n")
cat("Output structure:\n")
cat("  • public/total/total_exposure.geojson\n")
cat("  • public/daily/daily_YYYY-MM-DD.geojson\n")
cat("  • public/monthly/monthly_YYYY-MM.geojson\n")
cat("  • public/cumulative_snapshots/cumulative_YYYYMMDD.geojson\n")
cat("  • public/index.html\n")
cat("========================================\n")
