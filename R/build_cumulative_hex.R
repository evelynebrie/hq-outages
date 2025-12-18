# HQ Outages - HIERARCHICAL AGGREGATION SYSTEM v3
# With customer impact data and improved table logic

suppressPackageStartupMessages({
  library(sf)
  library(dplyr)
  library(jsonlite)
})

cat("=== HQ Outages Hierarchical Analysis v3 ===\n")
start_time <- Sys.time()

# Config
data_path <- "data/daily"
output_dir <- "public"

cat(">>> DEBUG: Writing outputs to:", normalizePath(output_dir, mustWork = FALSE), "\n")

HEX_SIZE <- 1000
SIMPLIFY <- 200

dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(file.path(output_dir, "daily"), recursive = TRUE, showWarnings = FALSE)
dir.create(file.path(output_dir, "monthly"), recursive = TRUE, showWarnings = FALSE)
dir.create(file.path(output_dir, "total"), recursive = TRUE, showWarnings = FALSE)

# =================================================================
# STEP 1: Identify files and extract datetime info
# =================================================================
cat("\n[1] Identifying dates to process...\n")

files <- list.files(data_path, pattern = "^polygons_.*\\.geojson$", full.names = TRUE)
if (length(files) == 0) stop("No polygon files found")

# Extract dates and hours from filenames
file_info <- lapply(basename(files), function(f) {
  m <- regmatches(f, regexpr("\\d{8}t\\d{6}", f, ignore.case = TRUE))
  if (length(m) > 0) {
    ts <- m[1]
    date_part <- substr(ts, 1, 8)
    hour_part <- substr(ts, 10, 11)
    list(
      date = sprintf("%s-%s-%s", substr(date_part,1,4), substr(date_part,5,6), substr(date_part,7,8)),
      hour = as.integer(hour_part),
      datetime = sprintf("%s-%s-%s %s:00", 
                        substr(date_part,1,4), substr(date_part,5,6), substr(date_part,7,8), hour_part)
    )
  } else {
    list(date = NA, hour = NA, datetime = NA)
  }
})

files_df <- data.frame(
  file = files,
  date = sapply(file_info, function(x) x$date),
  hour = sapply(file_info, function(x) x$hour),
  datetime = sapply(file_info, function(x) x$datetime),
  stringsAsFactors = FALSE
) %>% filter(!is.na(date))

unique_dates <- unique(files_df$date)
cat(sprintf("  Found %d files across %d dates\n", nrow(files_df), length(unique_dates)))

existing_dailies <- list.files(
  file.path(output_dir, "daily"), 
  pattern = "^daily_.*\\.geojson$"
)
existing_dates <- sub("daily_(.*)\\.geojson", "\\1", existing_dailies)

dates_to_process <- setdiff(unique_dates, existing_dates)
cat(sprintf("  %d dates already processed\n", length(existing_dates)))
cat(sprintf("  %d dates to process: %s\n", 
            length(dates_to_process),
            if(length(dates_to_process) <= 5) paste(dates_to_process, collapse=", ") else paste(c(head(dates_to_process, 3), "..."), collapse=", ")))

# =================================================================
# STEP 2: Process each date (only new ones)
# =================================================================
if (length(dates_to_process) > 0) {
  cat("\n[2] Processing daily summaries...\n")
  
  cat("  Creating reference hex grid...\n")
  first_file <- files_df$file[1]
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
  
  rm(first_data, bbox, hex_centroids)
  gc()
  
  cat(sprintf("  Template: %d hexagons\n", nrow(hex_grid_template)))
  
  for (date in dates_to_process) {
    cat(sprintf("  Processing %s...\n", date))
    
    date_files_info <- files_df %>% filter(date == !!date)
    date_files <- date_files_info$file
    cat(sprintf("    %d files\n", length(date_files)))
    
    hex_grid <- hex_grid_template
    hex_grid$count <- 0L
    hex_grid$hours_list <- vector("list", nrow(hex_grid))
    hex_grid$datetimes_list <- vector("list", nrow(hex_grid))
    
    for (i in seq_along(date_files)) {
      f <- date_files[i]
      hour_val <- date_files_info$hour[i]
      datetime_val <- date_files_info$datetime[i]
      
      tryCatch({
        polys <- st_read(f, quiet = TRUE) %>%
          st_transform(32618) %>%
          st_simplify(dTolerance = SIMPLIFY, preserveTopology = FALSE) %>%
          st_union()
        
        hits <- st_intersects(hex_grid, polys, sparse = TRUE)
        affected <- which(lengths(hits) > 0)
        
        for (idx in affected) {
          hex_grid$count[idx] <- hex_grid$count[idx] + 1L
          hex_grid$hours_list[[idx]] <- c(hex_grid$hours_list[[idx]], hour_val)
          hex_grid$datetimes_list[[idx]] <- c(hex_grid$datetimes_list[[idx]], datetime_val)
        }
        
        rm(polys, hits, affected)
      }, error = function(e) invisible(NULL))
    }
    
    hex_grid_filtered <- hex_grid %>%
      filter(count > 0) %>%
      mutate(
        date = date,
        n_files = length(date_files),
        hours_affected = sapply(hours_list, function(x) paste(sort(unique(x)), collapse = ",")),
        datetimes_affected = sapply(datetimes_list, function(x) paste(sort(unique(x)), collapse = ","))
      ) %>%
      select(-hours_list, -datetimes_list) %>%
      st_transform(4326)

    hex_grid_filtered <- st_make_valid(hex_grid_filtered)

    daily_path <- file.path(output_dir, "daily", sprintf("daily_%s.geojson", date))
    st_write(hex_grid_filtered, daily_path, delete_dsn = TRUE, quiet = TRUE)
    
    cat(sprintf("    → %d hexes affected (avg %.1f hours)\n", 
                nrow(hex_grid_filtered), mean(hex_grid_filtered$count)))
    
    gc()
  }
  
  rm(hex_grid_template)
  gc()
}

# =================================================================
# STEP 3: Aggregate monthly summaries
# =================================================================
cat("\n[3] Creating monthly summaries...\n")

all_daily_files <- list.files(
  file.path(output_dir, "daily"),
  pattern = "^daily_.*\\.geojson$",
  full.names = TRUE
)

if (length(all_daily_files) > 0) {
  daily_dates <- sub(".*daily_(\\d{4}-\\d{2})-\\d{2}\\.geojson", "\\1", all_daily_files)
  unique_months <- unique(daily_dates)
  
  cat(sprintf("  Processing %d months...\n", length(unique_months)))
  
  for (month in unique_months) {
    month_files <- all_daily_files[grepl(month, all_daily_files)]
    cat(sprintf("  %s: %d days\n", month, length(month_files)))
    
    month_data <- lapply(month_files, function(f) {
      st_read(f, quiet = TRUE)
    })
    
    combined <- bind_rows(month_data) %>%
      group_by(hex_id) %>%
      summarise(
        geometry = first(geometry),
        centroid_lon = first(centroid_lon),
        centroid_lat = first(centroid_lat),
        count = sum(count),
        n_files = sum(n_files),
        hours_affected = paste(unique(unlist(strsplit(paste(hours_affected, collapse = ","), ","))), collapse = ","),
        datetimes_affected = paste(unique(unlist(strsplit(paste(datetimes_affected, collapse = ","), ","))), collapse = ","),
        .groups = "drop"
      ) %>%
      st_as_sf() %>%
      mutate(month = month) %>%
      filter(count > 0)

    combined <- st_make_valid(combined)

    monthly_path <- file.path(output_dir, "monthly", sprintf("monthly_%s.geojson", month))
    st_write(combined, monthly_path, delete_dsn = TRUE, quiet = TRUE)
    
    cat(sprintf("    → %d hexes\n", nrow(combined)))
  }
}

# =================================================================
# STEP 4: Create TOTAL summary
# =================================================================
cat("\n[4] Creating total summary...\n")

if (length(all_daily_files) > 0) {
  cat("  Reading all daily summaries...\n")
  all_daily_data <- lapply(all_daily_files, function(f) {
    st_read(f, quiet = TRUE)
  })
  
  total_combined <- bind_rows(all_daily_data)

  total_combined <- total_combined %>%
    group_by(hex_id) %>%
    summarise(
      geometry = first(geometry),
      centroid_lon = first(centroid_lon),
      centroid_lat = first(centroid_lat),
      total_count = sum(count, na.rm = TRUE),
      total_files = sum(n_files, na.rm = TRUE),
      days_affected = n_distinct(date),
      hours_affected = paste(unique(unlist(strsplit(paste(hours_affected, collapse = ","), ","))), collapse = ","),
      datetimes_affected = paste(unique(unlist(strsplit(paste(datetimes_affected, collapse = ","), ","))), collapse = ","),
      .groups = "drop"
    ) %>%
    st_as_sf() %>%
    mutate(
      hours_count = total_count
    ) %>%
    filter(total_count > 0)

  total_combined <- st_make_valid(total_combined)

  cat(sprintf("  %d hexes affected across %d days\n", 
              nrow(total_combined), length(all_daily_files)))
  
  total_geojson <- file.path(output_dir, "total", "total_exposure.geojson")
  st_write(total_combined, total_geojson, delete_dsn = TRUE, quiet = TRUE)
  cat(sprintf("  Saved: %s\n", total_geojson))
  
  total_csv <- file.path(output_dir, "total", "total_stats.csv")
  total_combined %>%
    st_drop_geometry() %>%
    arrange(desc(hours_count)) %>%
    write.csv(total_csv, row.names = FALSE)
  cat(sprintf("  Saved: %s\n", total_csv))
  
  shp_dir <- file.path(output_dir, "total", "shapefile")
  dir.create(shp_dir, recursive = TRUE, showWarnings = FALSE)
  
  tryCatch({
    st_write(total_combined,
             file.path(shp_dir, "total_exposure.shp"),
             delete_dsn = TRUE, quiet = TRUE)
  }, error = function(e) {
    cat("  Warning: Could not create shapefile\n")
  })
  
  summary_stats <- list(
    total_days = length(all_daily_files),
    total_files = sum(total_combined$total_files[1]),
    total_hexes_affected = nrow(total_combined),
    mean_hours_affected = round(mean(total_combined$hours_count), 2),
    max_hours_affected = max(total_combined$hours_count),
    hex_size_m = HEX_SIZE,
    last_updated = as.character(Sys.Date()),
    processing_time_min = round(as.numeric(difftime(Sys.time(), start_time, units = "mins")), 1)
  )
  
  write_json(summary_stats, 
             file.path(output_dir, "total", "summary.json"),
             pretty = TRUE, auto_unbox = TRUE)
  
  cat(sprintf("  Mean hours affected: %.1f\n", summary_stats$mean_hours_affected))
  cat(sprintf("  Max hours affected: %d\n", summary_stats$max_hours_affected))
  
  # Verify critical files before HTML
  cat("\n[4b] Verifying all critical files saved...\n")
  
  critical_files <- c(
    "public/total/total_exposure.geojson",
    "public/total/total_stats.csv",
    "public/total/summary.json"
  )
  
  all_critical_saved <- TRUE
  for (f in critical_files) {
    exists <- file.exists(f)
    cat(sprintf("  %s %s\n", if(exists) "✓" else "✗", f))
    if (!exists) all_critical_saved <- FALSE
  }
  
  if (!all_critical_saved) {
    cat("\n❌ ERROR: Some critical files were not saved!\n")
    quit(status = 1)
  }
  
  cat("✅ All critical data files verified and saved\n")
}

# =================================================================
# STEP 5: Process customer impact data (NEW!)
# =================================================================
cat("\n[5] Processing customer impact data...\n")

customer_files <- list.files(data_path, pattern = "^outages_joined_.*\\.csv$", full.names = TRUE)

if (length(customer_files) > 0) {
  cat(sprintf("  Found %d customer impact files\n", length(customer_files)))
  
  customer_data_list <- list()
  files_processed <- 0
  files_failed <- 0
  
  for (cf in customer_files) {
    tryCatch({
      # Extract datetime from filename
      fname <- basename(cf)
      m <- regmatches(fname, regexpr("\\d{8}T\\d{6}", fname))
      if (length(m) > 0) {
        ts <- m[1]
        datetime_str <- sprintf("%s-%s-%s %s:%s",
                               substr(ts, 1, 4), substr(ts, 5, 6), substr(ts, 7, 8),
                               substr(ts, 10, 11), substr(ts, 12, 13))
        
        df <- read.csv(cf, stringsAsFactors = FALSE)
        if ("customers_sum" %in% names(df)) {
          total_customers <- sum(df$customers_sum, na.rm = TRUE)
          if (total_customers > 0) {
            customer_data_list[[length(customer_data_list) + 1]] <- data.frame(
              datetime = datetime_str,
              total_customers_affected = total_customers,
              stringsAsFactors = FALSE
            )
            files_processed <- files_processed + 1
          }
        } else {
          files_failed <- files_failed + 1
          if (files_failed <= 3) {
            cat(sprintf("    ⚠️ File missing 'customers_sum' column: %s (columns: %s)\n", 
                       fname, paste(names(df), collapse=", ")))
          }
        }
      }
    }, error = function(e) {
      files_failed <- files_failed + 1
      if (files_failed <= 3) {
        cat(sprintf("    ❌ Error processing %s: %s\n", basename(cf), e$message))
      }
    })
  }
  
  cat(sprintf("  Processed: %d files, Failed: %d files\n", files_processed, files_failed))
  
  if (length(customer_data_list) > 0) {
    customer_summary <- bind_rows(customer_data_list) %>%
      arrange(datetime)
    
    customer_json_path <- file.path(output_dir, "total", "customer_impact.json")
    write_json(customer_summary, customer_json_path, pretty = TRUE)
    cat(sprintf("  ✅ Saved: %s (%d records)\n", customer_json_path, nrow(customer_summary)))
  } else {
    cat("  ⚠️ No valid customer data extracted (all files had 0 customers or errors)\n")
  }
} else {
  cat("  No customer impact files found (this is optional)\n")
}

# =================================================================
# STEP 6: Create HTML viewer with all improvements
# =================================================================
cat("\n[6] Creating enhanced HTML viewer...\n")
cat("   (If this fails, your data files are already saved)\n")

hex_size_km <- HEX_SIZE / 1000
num_days <- length(all_daily_files)

html_generated <- tryCatch({
  
  # Load the customer impact data if it exists for embedding
  customer_data_js <- "null"
  customer_json_path <- file.path(output_dir, "total", "customer_impact.json")
  if (file.exists(customer_json_path)) {
    tryCatch({
      # Validate it's actually valid JSON before embedding
      test_json <- fromJSON(customer_json_path)
      if (is.list(test_json) || is.data.frame(test_json)) {
        customer_data_js <- readLines(customer_json_path) %>% paste(collapse = "\n")
        cat("  Customer impact data validated and embedded\n")
      } else {
        cat("  Customer impact file exists but is not valid, using null\n")
      }
    }, error = function(e) {
      cat("  Customer impact file validation failed:", e$message, "\n")
      cat("  Using null instead\n")
    })
  }
  
  html_parts <- list()

html_parts[[1]] <- paste0('<!DOCTYPE html>
<html lang="fr">
<head>
    <title>Pannes de courant cumulatives - Hydro-Québec</title>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width,initial-scale=1">
    <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
    <link rel="stylesheet" href="https://unpkg.com/leaflet-control-geocoder/dist/Control.Geocoder.css"/>
    <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
    <script src="https://unpkg.com/leaflet-control-geocoder/dist/Control.Geocoder.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        body{margin:0;padding:0;font-family:-apple-system,system-ui,sans-serif}
        #map{position:absolute;top:0;bottom:0;width:100%}
        .info{padding:12px;background:white;box-shadow:0 2px 10px rgba(0,0,0,0.1);border-radius:8px}
        .info h4{margin:0 0 8px;font-size:15px;font-weight:600}
        .info p{margin:4px 0;font-size:13px}
        .legend{line-height:20px;font-size:12px}
        .legend i{width:18px;height:18px;float:left;margin-right:8px;opacity:0.8;border-radius:2px}
        .controls{padding:10px;background:white;box-shadow:0 2px 10px rgba(0,0,0,0.1);border-radius:8px;max-width:250px}
        .controls h4{margin:0 0 8px;font-size:14px;font-weight:600}
        .controls label{display:block;margin:8px 0 4px;font-size:12px;font-weight:500}
        .controls select{width:100%;padding:6px;border-radius:4px;border:1px solid #ddd;font-size:12px}
        .detail-link{color:#0066cc;cursor:pointer;text-decoration:underline;font-size:12px;margin-top:8px;display:inline-block}
        .detail-link:hover{color:#0052a3}
        .detail-modal{display:none;position:fixed;z-index:2000;left:50%;top:50%;transform:translate(-50%,-50%);
                      background:white;padding:20px;border-radius:8px;box-shadow:0 4px 20px rgba(0,0,0,0.3);
                      max-width:600px;max-height:80vh;overflow-y:auto}
        .detail-modal.active{display:block}
        .modal-overlay{display:none;position:fixed;z-index:1999;left:0;top:0;width:100%;height:100%;
                       background:rgba(0,0,0,0.5)}
        .modal-overlay.active{display:block}
        .close-modal{float:right;font-size:24px;font-weight:bold;cursor:pointer;color:#999}
        .close-modal:hover{color:#333}
        .detail-table{width:100%;border-collapse:collapse;margin-top:10px;font-size:13px}
        .detail-table th,.detail-table td{text-align:left;padding:8px;border-bottom:1px solid #eee}
        .detail-table th{font-weight:600;background:#f5f5f5;position:sticky;top:0}
        .detail-table tfoot{font-weight:600;background:#f9f9f9}
        .sidebar{position:fixed;left:0;top:0;bottom:0;width:350px;background:white;box-shadow:2px 0 10px rgba(0,0,0,0.1);
                 transform:translateX(-350px);transition:transform 0.3s;z-index:1000;overflow-y:auto}
        .sidebar.active{transform:translateX(0)}
        .sidebar-toggle{position:fixed;left:10px;top:10px;z-index:1001;padding:10px 15px;background:white;
                        border:none;border-radius:4px;box-shadow:0 2px 10px rgba(0,0,0,0.1);cursor:pointer;font-size:14px}
        .sidebar-content{padding:20px}
        .sidebar h3{margin-top:0}
        .chart-container{margin:20px 0;height:300px}
        .welcome-modal{display:block;position:fixed;z-index:2001;left:50%;top:50%;transform:translate(-50%,-50%);
                       background:white;padding:30px;border-radius:8px;box-shadow:0 4px 20px rgba(0,0,0,0.3);
                       max-width:500px}
        .welcome-modal h2{margin-top:0;color:#0066cc}
        .welcome-modal button{padding:10px 20px;background:#0066cc;color:white;border:none;border-radius:4px;
                              cursor:pointer;font-size:14px;margin-top:15px}
        .welcome-modal button:hover{background:#0052a3}
    </style>
</head>
<body>
    <div id="map"></div>
    <div class="modal-overlay" id="modalOverlay"></div>
    <div class="modal-overlay active" id="welcomeOverlay"></div>
    
    <div class="welcome-modal" id="welcomeModal">
        <h2>Bienvenue!</h2>
        <p>Cette carte interactive présente les pannes de courant cumulatives dHydro-Québec.</p>
        <p><strong>Fonctionnalités:</strong></p>
        <ul>
            <li>Hexagones de 1km montrant les zones affectées</li>
            <li>Filtrage par date et heure</li>
            <li>Recherche dadresse ou code postal</li>
            <li>Statistiques des clients affectés (panneau latéral)</li>
            <li>Détails complets pour chaque hexagone</li>
        </ul>
        <p><em>Les données sont mises à jour quotidiennement.</em></p>
        <button onclick="closeWelcome()">Commencer</button>
    </div>
    
    <div class="detail-modal" id="detailModal">
        <span class="close-modal" onclick="closeDetailModal()">&times;</span>
        <div id="modalContent"></div>
    </div>
    
    <button class="sidebar-toggle" onclick="toggleSidebar()">☰ Statistiques</button>
    <div class="sidebar" id="sidebar">
        <div class="sidebar-content">
            <h3>Impact sur les clients</h3>
            <p id="sidebarInfo">Chargement...</p>
            <div class="chart-container">
                <canvas id="impactChart"></canvas>
            </div>
        </div>
    </div>
    
    <script>
        var customerData = ', customer_data_js, ';
        var map = L.map("map").setView([46.4, -72.7], 8);
        L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png",{attribution:"© OSM"}).addTo(map);
        
        var currentLayer;
        var allData = {};
        var impactChart = null;
        
        function closeWelcome() {
            document.getElementById("welcomeModal").style.display = "none";
            document.getElementById("welcomeOverlay").classList.remove("active");
        }
        
        function toggleSidebar() {
            document.getElementById("sidebar").classList.toggle("active");
        }
')

html_parts[[2]] <- "
        async function loadAllDailyData() {
            try {
                const response = await fetch('total/total_exposure.geojson');
                const data = await response.json();
                allData.total = data;
                
                const dates = new Set();
                data.features.forEach(f => {
                    if (f.properties.datetimes_affected) {
                        const datetimes = f.properties.datetimes_affected.split(',');
                        datetimes.forEach(dt => {
                            const date = dt.trim().split(' ')[0];
                            dates.add(date);
                        });
                    }
                });
                
                const dateSelect = document.getElementById('dateSelect');
                const sortedDates = Array.from(dates).sort();
                sortedDates.forEach(date => {
                    const opt = document.createElement('option');
                    opt.value = date;
                    opt.text = date;
                    dateSelect.appendChild(opt);
                });
                
                if (customerData && customerData.length > 0) {
                    initImpactChart();
                }
                
            } catch (e) {
                console.error('Error loading data:', e);
            }
        }
        
        function initImpactChart() {
            const ctx = document.getElementById('impactChart').getContext('2d');
            
            const dates = {};
            customerData.forEach(d => {
                const date = d.datetime.split(' ')[0];
                if (!dates[date]) dates[date] = 0;
                dates[date] += d.total_customers_affected;
            });
            
            impactChart = new Chart(ctx, {
                type: 'bar',
                data: {
                    labels: Object.keys(dates).sort(),
                    datasets: [{
                        label: 'Clients affectés',
                        data: Object.values(dates),
                        backgroundColor: '#cb181d'
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    scales: {
                        y: {
                            beginAtZero: true,
                            title: {
                                display: true,
                                text: 'Nombre de clients'
                            }
                        }
                    }
                }
            });
            
            const totalCustomers = customerData.reduce((sum, d) => sum + d.total_customers_affected, 0);
            document.getElementById('sidebarInfo').innerHTML = 
                '<p><strong>Total cumulé:</strong> ' + totalCustomers.toLocaleString() + ' clients</p>' +
                '<p><strong>Période:</strong> ' + Object.keys(dates).sort()[0] + ' à ' + 
                Object.keys(dates).sort()[Object.keys(dates).length-1] + '</p>';
        }
        
        function filterDataByDateTime(date, hour) {
            if (!allData.total) return null;
            
            if (date === 'all') {
                return allData.total;
            }
            
            const filtered = {
                type: 'FeatureCollection',
                features: allData.total.features.filter(f => {
                    if (!f.properties.datetimes_affected) return false;
                    const datetimes = f.properties.datetimes_affected.split(',').map(s => s.trim());
                    return datetimes.some(dt => {
                        if (hour === 'all') {
                            return dt.startsWith(date);
                        } else {
                            const targetDateTime = date + ' ' + hour.padStart(2, '0') + ':00';
                            return dt === targetDateTime;
                        }
                    });
                }).map(f => {
                    const datetimes = f.properties.datetimes_affected.split(',').map(s => s.trim());
                    let matchingCount = 0;
                    datetimes.forEach(dt => {
                        if (hour === 'all' && dt.startsWith(date)) {
                            matchingCount++;
                        } else if (hour !== 'all') {
                            const targetDateTime = date + ' ' + hour.padStart(2, '0') + ':00';
                            if (dt === targetDateTime) matchingCount++;
                        }
                    });
                    
                    return {
                        ...f,
                        properties: {
                            ...f.properties,
                            filtered_hours_count: matchingCount
                        }
                    };
                })
            };
            
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
                const hour = parseInt(time.split(':')[0]);
                byDate[date].push(hour);
            });
            
            let html = '<h3>Hexagone #' + hexId + '</h3>';
            html += '<p><strong>Centroïde:</strong> ' + props.centroid_lat.toFixed(6) + ', ' + props.centroid_lon.toFixed(6) + '</p>';
            html += '<table class=\"detail-table\">';
            html += '<thead><tr><th>Date</th><th>Première heure</th><th>Nombre d\\'heures</th></tr></thead><tbody>';
            
            let totalDays = 0;
            let totalHours = 0;
            
            Object.keys(byDate).sort().forEach(date => {
                const hours = byDate[date].sort((a, b) => a - b);
                const firstHour = hours[0];
                const numHours = hours.length;
                totalDays++;
                totalHours += numHours;
                html += '<tr><td>' + date + '</td><td>' + firstHour + ':00</td><td>' + numHours + '</td></tr>';
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
                           '<p><b>Jours analysés:</b> ", num_days, "</p>';
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
  cat("  ✓ HTML file created successfully\n")
  TRUE

}, error = function(e) {
  cat("\n❌ HTML GENERATION FAILED:\n")
  cat(sprintf("  Error: %s\n", e$message))
  cat("\n⚠️  Creating fallback HTML...\n")
  
  fallback_html <- sprintf('<!DOCTYPE html>
<html><head><title>HQ Outages Data</title></head>
<body>
<h1>HQ Outages Data Available</h1>
<p>Data processing completed. Download files:</p>
<ul>
<li><a href="total/total_exposure.geojson">GeoJSON</a></li>
<li><a href="total/total_stats.csv">CSV</a></li>
<li><a href="total/summary.json">Summary</a></li>
</ul>
</body></html>')
  
  writeLines(fallback_html, file.path(output_dir, "index.html"))
  cat("  ✓ Fallback HTML created\n")
  FALSE
})

# =================================================================
# Done!
# =================================================================
elapsed <- as.numeric(difftime(Sys.time(), start_time, units = "mins"))
cat(sprintf("\n✅ PROCESSING COMPLETE in %.1f minutes\n", elapsed))

cat("\n📊 DATA FILES SAVED:\n")
cat("  ✓ public/total/total_exposure.geojson\n")
cat("  ✓ public/total/total_stats.csv\n")
cat("  ✓ public/total/summary.json\n")
if (file.exists("public/total/customer_impact.json")) {
  cat("  ✓ public/total/customer_impact.json\n")
}
cat(sprintf("  ✓ public/daily/ - %d files\n", length(all_daily_files)))
cat(sprintf("  %s public/index.html\n", if(html_generated) "✓" else "⚠"))

cat("\n🔍 Final verification:\n")
required_files <- c(
  "public/total/total_exposure.geojson",
  "public/total/total_stats.csv",
  "public/index.html"
)

all_exist <- TRUE
for (f in required_files) {
  exists <- file.exists(f)
  cat(sprintf("  %s %s\n", if(exists) "✓" else "✗", f))
  if (!exists) all_exist <- FALSE
}

if (!all_exist) {
  cat("\n❌ ERROR: Some required files missing!\n")
  quit(status = 1)
}

cat("\n✅ ALL FILES VERIFIED AND READY\n")
cat("🚀 Ready for deployment!\n")
