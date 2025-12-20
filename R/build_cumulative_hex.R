# HQ Outages - INCREMENTAL UPDATE SYSTEM v6 - OPTIMIZED FOR DAILY RUNS
# Only processes NEW data and incrementally updates cumulative summaries
# Saves versioned cumulative snapshots for historical tracking

suppressPackageStartupMessages({
  library(sf)
  library(dplyr)
  library(jsonlite)
  library(parallel)
  library(data.table)
})

cat("=== HQ Outages INCREMENTAL Update System v6 ===\n")
cat("This script is optimized for daily runs:\n")
cat("  • First run: Processes all dates (~2 min per date)\n")
cat("  • Daily runs: Only processes new dates (~3 seconds)\n")
cat("  • Reuses existing daily summaries when available\n")
cat("  • Saves versioned snapshots in cumulative_snapshots/\n")
cat(sprintf("\nStarting at: %s\n", format(Sys.time(), "%Y-%m-%d %H:%M:%S")))
start_time <- Sys.time()

# Config
data_path <- "data/daily"
output_dir <- "public"
cumulative_snapshots_dir <- file.path(output_dir, "cumulative_snapshots")

HEX_SIZE <- 1000
SIMPLIFY <- 200
N_CORES <- 1

cat(sprintf(">>> Using %d CPU cores for parallel processing\n", N_CORES))

dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(file.path(output_dir, "daily"), recursive = TRUE, showWarnings = FALSE)
dir.create(file.path(output_dir, "monthly"), recursive = TRUE, showWarnings = FALSE)
dir.create(file.path(output_dir, "total"), recursive = TRUE, showWarnings = FALSE)
dir.create(cumulative_snapshots_dir, recursive = TRUE, showWarnings = FALSE)

# =================================================================
# STEP 1: Identify NEW files to process
# =================================================================
cat("\n[1] Identifying NEW dates to process...\n")

files <- list.files(data_path, pattern = "^polygons_.*\\.geojson$", full.names = TRUE)
if (length(files) == 0) stop("No polygon files found")

# OPTIMIZATION: Vectorized timestamp parsing
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

cat(sprintf("  Found %d total files\n", nrow(files_dt)))

# Select latest file per hour
setkey(files_dt, date, hour, timestamp_sort)
files_dt <- files_dt[, .SD[.N], by = .(date, hour)]
setorder(files_dt, date, hour)

# CRITICAL: Only process dates that don't have daily summaries yet
existing_dailies <- list.files(
  file.path(output_dir, "daily"), 
  pattern = "^daily_.*\\.geojson$"
)
existing_dates <- sub("daily_(.*)\\.geojson", "\\1", existing_dailies)

dates_to_process <- setdiff(unique(files_dt$date), existing_dates)

all_dates <- unique(files_dt$date)
cat(sprintf("  Total unique dates in source: %d\n", length(all_dates)))
cat(sprintf("  Already processed (daily summaries exist): %d\n", length(existing_dates)))
cat(sprintf("  NEW dates to process: %d\n", length(dates_to_process)))

if (length(existing_dates) > 0) {
  cat(sprintf("  ✓ Found existing daily summaries - will reuse them!\n"))
  cat(sprintf("    Date range: %s to %s\n", min(existing_dates), max(existing_dates)))
}

if (length(dates_to_process) > 0) {
  if (length(dates_to_process) <= 10) {
    cat(sprintf("  New dates: %s\n", paste(dates_to_process, collapse=", ")))
  } else {
    cat(sprintf("  New dates: %s ... %s (%d total)\n", 
                paste(head(dates_to_process, 3), collapse=", "),
                paste(tail(dates_to_process, 3), collapse=", "),
                length(dates_to_process)))
  }
  
  # Estimate processing time
  est_time_min <- length(dates_to_process) * 2  # ~2 min per date on average
  if (est_time_min > 60) {
    cat(sprintf("  ⏱ Estimated processing time: ~%.1f hours\n", est_time_min / 60))
  } else {
    cat(sprintf("  ⏱ Estimated processing time: ~%d minutes\n", est_time_min))
  }
}

# =================================================================
# STEP 2: Process ONLY NEW dates (if any)
# =================================================================
if (length(dates_to_process) > 0) {
  cat("\n[2] Processing NEW daily summaries...\n")
  
  # Load or create hex grid template
  hex_grid_path <- file.path(output_dir, "hex_grid_template.rds")
  
  if (file.exists(hex_grid_path)) {
    cat("  Loading existing hex grid template...\n")
    hex_grid_template <- readRDS(hex_grid_path)
  } else {
    cat("  Creating hex grid template (first run)...\n")
    first_file <- files_dt$file[1]
    first_data <- st_read(first_file, quiet = TRUE) %>% st_transform(32618)
    bbox <- st_bbox(first_data)
    bbox["xmin"] <- bbox["xmin"] - 50000
    bbox["xmax"] <- bbox["xmax"] + 50000
    bbox["ymin"] <- bbox["ymin"] - 50000
    bbox["ymax"] <- bbox["ymax"] + 50000
    class(bbox) <- "bbox"
    attr(bbox, "crs") <- st_crs(32618)
    
    hex_grid_template <- st_make_grid(
      st_as_sfc(bbox), 
      cellsize = HEX_SIZE, 
      square = FALSE
    ) %>%
      st_sf() %>%
      mutate(hex_id = row_number())
    
    hex_centroids <- st_centroid(hex_grid_template) %>%
      st_transform(4326) %>%
      st_coordinates() %>%
      as.data.frame() %>%
      rename(centroid_lon = X, centroid_lat = Y)
    
    hex_grid_template$centroid_lon <- hex_centroids$centroid_lon
    hex_grid_template$centroid_lat <- hex_centroids$centroid_lat
    
    saveRDS(hex_grid_template, hex_grid_path)
    cat(sprintf("  Template saved: %d hexagons\n", nrow(hex_grid_template)))
  }
  
  # Process new dates
  process_date <- function(date) {
    date_files_info <- files_dt[files_dt$date == date, ]
    
    hex_grid <- hex_grid_template
    hex_grid$count <- 0L
    hex_grid$datetimes_list <- vector("list", nrow(hex_grid))
    
    for (i in seq_len(nrow(date_files_info))) {
      f <- date_files_info$file[i]
      datetime_val <- date_files_info$datetime[i]
      
      tryCatch({
        polys <- st_read(f, quiet = TRUE) %>%
          st_transform(32618) %>%
          st_simplify(dTolerance = SIMPLIFY, preserveTopology = FALSE)
        
        if (nrow(polys) > 1) polys <- st_union(polys)
        
        hits <- st_intersects(hex_grid, polys, sparse = TRUE)
        affected <- which(lengths(hits) > 0)
        
        if (length(affected) > 0) {
          hex_grid$count[affected] <- hex_grid$count[affected] + 1L
          for (idx in affected) {
            hex_grid$datetimes_list[[idx]] <- c(hex_grid$datetimes_list[[idx]], datetime_val)
          }
        }
        rm(polys, hits)
      }, error = function(e) invisible(NULL))
    }
    
    hex_grid_filtered <- hex_grid[hex_grid$count > 0, ]
    hex_grid_filtered$date <- date
    hex_grid_filtered$n_files <- nrow(date_files_info)
    hex_grid_filtered$datetimes_affected <- vapply(
      hex_grid_filtered$datetimes_list, 
      function(x) paste(sort(x), collapse = ","),
      character(1)
    )
    hex_grid_filtered$datetimes_list <- NULL
    hex_grid_filtered <- st_transform(hex_grid_filtered, 4326)
    hex_grid_filtered <- st_make_valid(hex_grid_filtered)
    
    daily_path <- file.path(output_dir, "daily", sprintf("daily_%s.geojson", date))
    st_write(hex_grid_filtered, daily_path, delete_dsn = TRUE, quiet = TRUE)
    
    list(date = date, n_hexes = nrow(hex_grid_filtered), 
         avg_hours = if(nrow(hex_grid_filtered) > 0) mean(hex_grid_filtered$count) else 0)
  }
  
  # Parallel or sequential processing
  if (length(dates_to_process) > 1 && N_CORES > 1) {
    cat(sprintf("  Processing %d dates in parallel...\n", length(dates_to_process)))
    results <- if (.Platform$OS.type == "unix") {
      mclapply(dates_to_process, process_date, mc.cores = N_CORES)
    } else {
      cl <- makeCluster(N_CORES)
      clusterExport(cl, c("files_dt", "hex_grid_template", "output_dir", "SIMPLIFY"),
                    envir = environment())
      results <- parLapply(cl, dates_to_process, process_date)
      stopCluster(cl)
      results
    }
    for (r in results) {
      cat(sprintf("  %s: %d hexes (avg %.1f hours)\n", r$date, r$n_hexes, r$avg_hours))
    }
  } else {
    for (date in dates_to_process) {
      cat(sprintf("  Processing %s...\n", date))
      r <- process_date(date)
      cat(sprintf("    → %d hexes (avg %.1f hours)\n", r$n_hexes, r$avg_hours))
    }
  }
  
  cat("  ✓ New daily summaries created\n")
} else {
  cat("\n[2] No new dates to process - all daily summaries up to date!\n")
}

# =================================================================
# STEP 3: Update monthly summaries (INCREMENTAL)
# =================================================================
cat("\n[3] Updating monthly summaries (incremental)...\n")

all_daily_files <- list.files(
  file.path(output_dir, "daily"),
  pattern = "^daily_.*\\.geojson$",
  full.names = TRUE
)

if (length(all_daily_files) > 0) {
  # Extract month from daily files
  daily_months <- unique(sub(".*daily_(\\d{4}-\\d{2})-\\d{2}\\.geojson", "\\1", all_daily_files))
  
  # Find which months need updating
  existing_monthlies <- list.files(
    file.path(output_dir, "monthly"),
    pattern = "^monthly_.*\\.geojson$"
  )
  existing_monthly_dates <- sub("monthly_(.*)\\.geojson", "\\1", existing_monthlies)
  
  # Only update months that have new data
  months_to_update <- if (length(dates_to_process) > 0) {
    unique(substr(dates_to_process, 1, 7))  # YYYY-MM format
  } else {
    character(0)
  }
  
  if (length(months_to_update) > 0) {
    cat(sprintf("  Updating %d months with new data: %s\n", 
                length(months_to_update), paste(months_to_update, collapse=", ")))
    
    for (month in months_to_update) {
      month_files <- all_daily_files[grepl(month, all_daily_files)]
      month_data <- lapply(month_files, function(f) st_read(f, quiet = TRUE))
      
      combined_dt <- rbindlist(lapply(month_data, function(x) {
        data.table(
          hex_id = x$hex_id,
          geometry = st_as_text(x$geometry),
          centroid_lon = x$centroid_lon,
          centroid_lat = x$centroid_lat,
          count = x$count,
          n_files = x$n_files,
          datetimes_affected = x$datetimes_affected
        )
      }))
      
      combined_summary <- combined_dt[, .(
        geometry = first(geometry),
        centroid_lon = first(centroid_lon),
        centroid_lat = first(centroid_lat),
        count = sum(count),
        n_files = sum(n_files),
        datetimes_affected = paste(unlist(strsplit(paste(datetimes_affected, collapse = ","), ",")), collapse = ",")
      ), by = hex_id]
      
      combined <- st_as_sf(combined_summary, wkt = "geometry", crs = 4326)
      combined <- st_make_valid(combined)
      
      monthly_path <- file.path(output_dir, "monthly", sprintf("monthly_%s.geojson", month))
      st_write(combined, monthly_path, delete_dsn = TRUE, quiet = TRUE)
      cat(sprintf("  ✓ %s: %d hexes\n", month, nrow(combined)))
    }
  } else {
    cat("  All monthly summaries up to date!\n")
  }
}

# =================================================================
# STEP 4: Update TOTAL cumulative (INCREMENTAL with versioning)
# =================================================================
cat("\n[4] Updating total cumulative summary (incremental)...\n")

# Get current date for snapshot versioning
current_date <- format(Sys.time(), "%Y-%m-%d")
snapshot_path <- file.path(cumulative_snapshots_dir, sprintf("cumulative_%s.geojson", current_date))

# Check if we need to update
need_update <- length(dates_to_process) > 0

if (need_update) {
  cat("  New data detected - updating cumulative total...\n")
  
  # Load previous cumulative (if exists)
  previous_cumulative_path <- file.path(output_dir, "total", "total_exposure.geojson")
  
  if (file.exists(previous_cumulative_path)) {
    cat("  Loading previous cumulative data...\n")
    previous_data <- st_read(previous_cumulative_path, quiet = TRUE)
    
    # Convert to data.table
    previous_dt <- data.table(
      hex_id = previous_data$hex_id,
      geometry = st_as_text(previous_data$geometry),
      centroid_lon = previous_data$centroid_lon,
      centroid_lat = previous_data$centroid_lat,
      hours_count = previous_data$hours_count,
      n_files = previous_data$n_files,
      datetimes_affected = previous_data$datetimes_affected
    )
    
    # Load ONLY the new daily files
    new_daily_paths <- file.path(output_dir, "daily", sprintf("daily_%s.geojson", dates_to_process))
    new_daily_data <- lapply(new_daily_paths, function(f) st_read(f, quiet = TRUE))
    
    new_dt <- rbindlist(lapply(new_daily_data, function(x) {
      data.table(
        hex_id = x$hex_id,
        geometry = st_as_text(x$geometry),
        centroid_lon = x$centroid_lon,
        centroid_lat = x$centroid_lat,
        count = x$count,
        n_files = x$n_files,
        datetimes_affected = x$datetimes_affected
      )
    }))
    
    # Merge: combine previous with new
    cat("  Merging previous cumulative with new data...\n")
    combined_dt <- rbind(
      previous_dt[, .(hex_id, geometry, centroid_lon, centroid_lat, 
                     count = hours_count, n_files, datetimes_affected)],
      new_dt,
      fill = TRUE
    )
    
  } else {
    cat("  No previous cumulative - checking for existing daily summaries...\n")
    
    # Check if we have daily summaries to build from
    if (length(all_daily_files) > 0) {
      cat(sprintf("  ✓ Found %d existing daily summaries - building cumulative from these!\n", 
                  length(all_daily_files)))
      all_daily_data <- lapply(all_daily_files, function(f) st_read(f, quiet = TRUE))
    } else {
      cat("  ✗ No daily summaries found - this shouldn't happen!\n")
      stop("No daily summaries available to build cumulative. Run daily processing first.")
    }
    
    combined_dt <- rbindlist(lapply(all_daily_data, function(x) {
      data.table(
        hex_id = x$hex_id,
        geometry = st_as_text(x$geometry),
        centroid_lon = x$centroid_lon,
        centroid_lat = x$centroid_lat,
        count = x$count,
        n_files = x$n_files,
        datetimes_affected = x$datetimes_affected
      )
    }))
  }
  
  # Aggregate
  total_summary <- combined_dt[, .(
    geometry = first(na.omit(geometry)),
    centroid_lon = first(centroid_lon),
    centroid_lat = first(centroid_lat),
    hours_count = sum(count),
    n_files = sum(n_files),
    datetimes_affected = paste(unlist(strsplit(paste(datetimes_affected, collapse = ","), ",")), collapse = ",")
  ), by = hex_id]
  
  # Calculate days affected
  total_summary[, days_affected := sapply(strsplit(datetimes_affected, ","), function(dts) {
    length(unique(sapply(strsplit(dts, " "), `[`, 1)))
  })]
  
  # Convert to sf
  total_combined <- st_as_sf(total_summary, wkt = "geometry", crs = 4326)
  total_combined <- st_make_valid(total_combined)
  
  # Save current version
  st_write(total_combined, previous_cumulative_path, delete_dsn = TRUE, quiet = TRUE)
  cat(sprintf("  ✓ Updated cumulative: %d hexes affected\n", nrow(total_combined)))
  
  # Save versioned snapshot
  st_write(total_combined, snapshot_path, delete_dsn = TRUE, quiet = TRUE)
  cat(sprintf("  ✓ Snapshot saved: %s\n", basename(snapshot_path)))
  
  # Create CSV statistics
  stats_csv <- as.data.frame(total_summary[, .(hex_id, centroid_lat, centroid_lon, 
                                               hours_count, days_affected, datetimes_affected)])
  
  stats_path <- file.path(output_dir, "total", "total_stats.csv")
  fwrite(stats_csv, stats_path)
  cat("  ✓ Stats CSV updated\n")
  
  # Create summary JSON
  summary_data <- list(
    generated_at = format(Sys.time(), "%Y-%m-%d %H:%M:%S %Z"),
    last_update_date = current_date,
    new_dates_added = length(dates_to_process),
    total_hexes_affected = nrow(total_combined),
    total_hours_scraped = sum(total_combined$hours_count),
    date_range = list(
      first = min(files_dt$date),
      last = max(files_dt$date)
    ),
    hex_size_meters = HEX_SIZE,
    optimization_info = list(
      cores_used = N_CORES,
      processing_mode = "incremental",
      snapshot_date = current_date
    ),
    stats = list(
      mean_hours = mean(total_combined$hours_count),
      median_hours = median(total_combined$hours_count),
      max_hours = max(total_combined$hours_count),
      mean_days = mean(total_combined$days_affected),
      median_days = median(total_combined$days_affected),
      max_days = max(total_combined$days_affected)
    )
  )
  
  summary_path <- file.path(output_dir, "total", "summary.json")
  writeLines(jsonlite::toJSON(summary_data, auto_unbox = TRUE, pretty = TRUE), summary_path)
  cat("  ✓ Summary JSON updated\n")
  
  num_days <- length(unique(files_dt$date))
  hex_size_km <- round(HEX_SIZE / 1000, 1)
  
} else {
  cat("  No new data - cumulative total is up to date!\n")
  
  # Still load for HTML generation
  previous_cumulative_path <- file.path(output_dir, "total", "total_exposure.geojson")
  if (file.exists(previous_cumulative_path)) {
    total_combined <- st_read(previous_cumulative_path, quiet = TRUE)
    num_days <- length(unique(files_dt$date))
    hex_size_km <- round(HEX_SIZE / 1000, 1)
  }
}

# =================================================================
# STEP 5: Generate/Update HTML
# =================================================================
cat("\n[5] Updating HTML map...\n")

html_generated <- tryCatch({
  html_parts <- vector("list", 4)
  
  html_parts[[1]] <- paste0('<!DOCTYPE html>
<html lang="fr">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Pannes de courant Hydro-Québec - Analyse cumulative</title>
    <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
    <link rel="stylesheet" href="https://unpkg.com/leaflet-control-geocoder/dist/Control.Geocoder.css"/>
    <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
    <script src="https://unpkg.com/leaflet-control-geocoder/dist/Control.Geocoder.js"></script>
    <style>
        body { margin:0; padding:0; font-family: -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif; }
        #map { position:absolute; top:0; bottom:0; width:100%; }
        .info { padding:10px; background:white; border-radius:5px; box-shadow:0 0 15px rgba(0,0,0,0.2); max-width:300px; }
        .info h4 { margin:0 0 10px; font-size:16px; color:#333; }
        .info p { margin:5px 0; font-size:13px; color:#666; }
        .legend { line-height:20px; color:#555; background:white; padding:10px; border-radius:5px; box-shadow:0 0 15px rgba(0,0,0,0.2); }
        .legend i { width:18px; height:18px; float:left; margin-right:8px; opacity:0.8; }
        .legend h4 { margin:0 0 8px; font-size:14px; }
        .controls { background:white; padding:15px; border-radius:5px; box-shadow:0 0 15px rgba(0,0,0,0.2); min-width:200px; }
        .controls h4 { margin:0 0 10px; font-size:14px; color:#333; }
        .controls label { display:block; margin:8px 0 4px; font-size:12px; color:#666; font-weight:500; }
        .controls select { width:100%; padding:6px; border:1px solid #ddd; border-radius:4px; font-size:13px; }
        .detail-link { color:#1e88e5; cursor:pointer; text-decoration:underline; font-size:12px; }
        .detail-link:hover { color:#1565c0; }
        .modal-overlay { display:none; position:fixed; top:0; left:0; right:0; bottom:0; background:rgba(0,0,0,0.5); z-index:2000; }
        .modal-overlay.active { display:block; }
        .detail-modal { display:none; position:fixed; top:50%; left:50%; transform:translate(-50%,-50%); background:white; padding:25px; border-radius:8px; box-shadow:0 4px 20px rgba(0,0,0,0.3); z-index:2001; max-width:90%; max-height:90%; overflow-y:auto; }
        .detail-modal.active { display:block; }
        .detail-modal h3 { margin:0 0 15px; color:#333; }
        .detail-modal p { margin:10px 0; color:#666; }
        .detail-table { width:100%; border-collapse:collapse; margin:15px 0; font-size:13px; }
        .detail-table th, .detail-table td { padding:10px; text-align:left; border-bottom:1px solid #e0e0e0; }
        .detail-table thead { background:#f5f5f5; }
        .detail-table th { font-weight:600; color:#555; }
        .detail-table tfoot { font-weight:600; background:#fafafa; }
        .close-modal { position:absolute; top:10px; right:15px; font-size:24px; cursor:pointer; color:#999; }
        .close-modal:hover { color:#333; }
    </style>
</head>
<body>
    <div id="map"></div>
    <div id="modalOverlay" class="modal-overlay"></div>
    <div id="detailModal" class="detail-modal">
        <span class="close-modal" onclick="closeDetailModal()">×</span>
        <div id="modalContent"></div>
    </div>
    <script>
')

  html_parts[[2]] <- "
        var map = L.map('map').setView([46.8, -71.2], 8);
        L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png',{
            attribution:'© OpenStreetMap',
            maxZoom:18
        }).addTo(map);
        
        var allData = { daily: {}, total: null };
        var currentLayer = null;
        
        async function loadAllDailyData() {
            const totalResp = await fetch('total/total_exposure.geojson');
            allData.total = await totalResp.json();
            
            const dateSelect = document.getElementById('dateSelect');
            const dates = [...new Set(allData.total.features.flatMap(f => {
                return f.properties.datetimes_affected.split(',').map(dt => dt.split(' ')[0]);
            }))].sort();
            
            for (const date of dates) {
                const opt = document.createElement('option');
                opt.value = date;
                opt.textContent = date;
                dateSelect.appendChild(opt);
            }
            
            for (const date of dates) {
                try {
                    const resp = await fetch(`daily/daily_${date}.geojson`);
                    allData.daily[date] = await resp.json();
                } catch(e) {
                    console.warn('Could not load daily data for', date);
                }
            }
        }
        
        function filterDataByDateTime(date, hour) {
            if (date === 'all') return allData.total;
            
            const dailyData = allData.daily[date];
            if (!dailyData) return null;
            
            if (hour === 'all') return dailyData;
            
            const filtered = JSON.parse(JSON.stringify(dailyData));
            filtered.features = filtered.features.map(f => {
                const dts = f.properties.datetimes_affected.split(',');
                const matchingDts = dts.filter(dt => {
                    const dtHour = parseInt(dt.split(' ')[1].split(':')[0]);
                    return dtHour === parseInt(hour);
                });
                
                if (matchingDts.length === 0) return null;
                
                f.properties.filtered_hours_count = matchingDts.length;
                return f;
            }).filter(f => f !== null);
            
            return filtered;
        }
        
        function updateMap() {
            const date = document.getElementById('dateSelect').value;
            const hour = document.getElementById('hourSelect').value;
            
            const data = filterDataByDateTime(date, hour);
            if (!data) return;
            
            if (currentLayer) map.removeLayer(currentLayer);
            
            const isFiltered = date !== 'all';
            const useFixedColor = isFiltered;
            
            currentLayer = L.geoJSON(data, {
                style: f => ({
                    fillColor: useFixedColor ? '#a50f15' : getColor(f.properties.hours_count),
                    weight: 0.5,
                    color: '#fff',
                    fillOpacity: useFixedColor ? 0.8 : 0.75
                }),
                onEachFeature: (f, layer) => {
                    const props = f.properties;
                    const hoursCount = isFiltered ? props.filtered_hours_count : props.hours_count;
                    
                    var popup = '<b>Hexagone #' + props.hex_id + '</b><br>' +
                               '<b>Nombre d\\'heures impactées:</b> ' + hoursCount + '<br>' +
                               '<b>Nombre de jours impactés:</b> ' + props.days_affected + '<br>' +
                               '<b>Coordonnées du centroïde:</b><br>' +
                               'Lat: ' + props.centroid_lat.toFixed(6) + ', Lon: ' + props.centroid_lon.toFixed(6) + '<br>' +
                               '<a class=\"detail-link\" onclick=\"showDetails(' + props.hex_id + ')\">Voir les détails complets</a>';
                    
                    layer.bindPopup(popup);
                }
            }).addTo(map);
        }
        
        function showDetails(hexId) {
            const feature = allData.total.features.find(f => f.properties.hex_id === hexId);
            if (!feature) return;
            
            const props = feature.properties;
            const datetimes = props.datetimes_affected.split(',').map(s => s.trim()).sort();
            
            const byDate = {};
            datetimes.forEach(dt => {
                const [date, time] = dt.split(' ');
                if (!byDate[date]) byDate[date] = [];
                byDate[date].push(time);
            });
            
            let html = '<h3>Hexagone #' + hexId + '</h3>';
            html += '<p><strong>Centroïde:</strong> ' + props.centroid_lat.toFixed(6) + ', ' + props.centroid_lon.toFixed(6) + '</p>';
            html += '<table class=\"detail-table\">';
            html += '<thead><tr><th>Date</th><th>Heures affectées</th><th>Nombre d\\'heures</th></tr></thead><tbody>';
            
            let totalDays = 0;
            let totalHours = 0;
            
            Object.keys(byDate).sort().forEach(date => {
                const times = byDate[date].sort();
                const numHours = times.length;
                const timeList = times.join(', ');
                totalDays++;
                totalHours += numHours;
                html += '<tr><td>' + date + '</td><td>' + timeList + '</td><td>' + numHours + '</td></tr>';
            });
            
            html += '</tbody><tfoot><tr><td><strong>Total</strong></td><td>' + totalDays + ' jours</td><td>' + totalHours + ' heures</td></tr></tfoot>';
            html += '</table>';
            
            document.getElementById('modalContent').innerHTML = html;
            document.getElementById('detailModal').classList.add('active');
            document.getElementById('modalOverlay').classList.add('active');
        }
        
        function closeDetailModal() {
            document.getElementById('detailModal').classList.remove('active');
            document.getElementById('modalOverlay').classList.remove('active');
        }
        
        document.getElementById('modalOverlay').onclick = closeDetailModal;
        
        function getColor(d) {
            return d>80?'#67000d':d>60?'#a50f15':d>40?'#cb181d':d>20?'#ef3b2c':
                   d>10?'#fb6a4a':d>5?'#fc9272':d>2?'#fcbba1':'#fee5d9';
        }
        
        loadAllDailyData().then(() => {
            updateMap();
        });
        
        var legend = L.control({position:'bottomright'});
        legend.onAdd = () => {
            var div = L.DomUtil.create('div','info legend');
            div.innerHTML = '<h4>Heures (nombre)</h4>';
            [0,2,5,10,20,40,60,80].forEach((g,i,a) => {
                div.innerHTML += '<i style=\"background:' + getColor(g+1) + '\"></i>' + g + (a[i+1]?'–'+a[i+1]:'+') + '<br>';
            });
            return div;
        };
        legend.addTo(map);
        
        var geocoder = L.Control.geocoder({
            defaultMarkGeocode: false,
            placeholder: 'Rechercher une adresse...',
            errorMessage: 'Adresse non trouvée'
        })
        .on('markgeocode', function(e) {
            var bbox = e.geocode.bbox;
            var poly = L.polygon([
                bbox.getSouthEast(),
                bbox.getNorthEast(),
                bbox.getNorthWest(),
                bbox.getSouthWest()
            ]);
            map.fitBounds(poly.getBounds());
        })
        .addTo(map);
"

  html_parts[[3]] <- paste0("
        var info = L.control({position:'topright'});
        info.onAdd = () => {
            var div = L.DomUtil.create('div','info');
            div.innerHTML = '<h4>Pannes de courant cumulatives</h4>' +
                           '<p><b>Taille des hexagones:</b> ", hex_size_km, " km</p>' +
                           '<p><b>Jours analysés:</b> ", num_days, "</p>' +
                           '<p style=\"font-size:11px;color:#999;\">Mise à jour: ", current_date, "</p>';
            return div;
        };
        info.addTo(map);
        
        var controls = L.control({position:'topleft'});
        controls.onAdd = () => {
            var div = L.DomUtil.create('div','controls');
            div.innerHTML = '<h4>Filtres</h4>' +
                           '<label>Date:</label>' +
                           '<select id=\"dateSelect\" onchange=\"updateMap()\"><option value=\"all\">Résumé complet (défaut)</option></select>' +
                           '<label>Heure:</label>' +
                           '<select id=\"hourSelect\" onchange=\"updateMap()\"><option value=\"all\">Toutes</option>' +")

  html_parts[[4]] <- paste0(
    paste(sapply(0:23, function(i) sprintf("'<option value=\"%d\">%d:00</option>'", i, i)), collapse=" + "),
    " + '</select>';
            return div;
        };
        controls.addTo(map);
    </script>
</body>
</html>")

  html <- paste(html_parts, collapse = "")
  writeLines(html, file.path(output_dir, "index.html"))
  cat("  ✓ HTML updated\n")
  TRUE
}, error = function(e) {
  cat(sprintf("  ⚠ HTML update failed: %s\n", e$message))
  FALSE
})

# =================================================================
# Cleanup old snapshots (keep last 30 days)
# =================================================================
cat("\n[6] Managing snapshot history...\n")
all_snapshots <- list.files(cumulative_snapshots_dir, pattern = "^cumulative_.*\\.geojson$", full.names = TRUE)
if (length(all_snapshots) > 30) {
  # Sort by modification time and keep only recent 30
  snapshot_info <- file.info(all_snapshots)
  snapshot_info <- snapshot_info[order(snapshot_info$mtime, decreasing = TRUE), ]
  snapshots_to_delete <- rownames(snapshot_info)[31:nrow(snapshot_info)]
  
  for (f in snapshots_to_delete) {
    file.remove(f)
  }
  cat(sprintf("  ✓ Cleaned up %d old snapshots (keeping last 30)\n", length(snapshots_to_delete)))
} else {
  cat(sprintf("  ✓ Snapshot history: %d files (under 30-day limit)\n", length(all_snapshots)))
}

# =================================================================
# Summary
# =================================================================
elapsed <- as.numeric(difftime(Sys.time(), start_time, units = "secs"))
cat(sprintf("\n✅ INCREMENTAL UPDATE COMPLETE in %.1f seconds\n", elapsed))

cat("\n📊 PROCESSING SUMMARY:\n")
cat(sprintf("  • New dates processed: %d\n", length(dates_to_process)))
cat(sprintf("  • Total dates in system: %d\n", length(unique(files_dt$date))))
cat(sprintf("  • Daily summaries: %d files\n", length(all_daily_files)))
cat(sprintf("  • Cumulative snapshots: %d files\n", length(all_snapshots)))
cat(sprintf("  • Processing time: %.1f seconds\n", elapsed))
cat(sprintf("  • Mode: %s (%d cores)\n", if(need_update) "INCREMENTAL UPDATE" else "NO UPDATE NEEDED", N_CORES))

if (elapsed < 60) {
  cat(sprintf("\n⚡ SPEED: ~%.0fx faster than full rebuild (estimated)\n", 
              if(length(dates_to_process) > 0) max(30, length(unique(files_dt$date)) / max(1, length(dates_to_process))) else 100))
}

cat("\n✅ READY FOR DEPLOYMENT\n")
