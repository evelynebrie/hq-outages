# HQ Outages - HIERARCHICAL AGGREGATION SYSTEM (MODIFIED)
# Changes:
# 1. Hex size changed to 1km (1000m)
# 2. Score = number of hours (files) hex appears in, not percentage
# 3. Added centroid coordinates for each hex
# 4. Added day/hour tracking for each hex

suppressPackageStartupMessages({
  library(sf)
  library(dplyr)
  library(jsonlite)
})

cat("=== HQ Outages Hierarchical Analysis (Modified) ===\n")
start_time <- Sys.time()

# Config
data_path <- "data/daily"
output_dir <- "public"

cat(">>> DEBUG: Writing outputs to:", normalizePath(output_dir, mustWork = FALSE), "\n")

# MODIFIED: Hex size changed from 5000m to 1000m (1km)
HEX_SIZE <- 1000
SIMPLIFY <- 200

dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(file.path(output_dir, "daily"), recursive = TRUE, showWarnings = FALSE)
dir.create(file.path(output_dir, "monthly"), recursive = TRUE, showWarnings = FALSE)
dir.create(file.path(output_dir, "total"), recursive = TRUE, showWarnings = FALSE)

# =================================================================
# STEP 1: Identify which dates need processing
# =================================================================
cat("\n[1] Identifying dates to process...\n")

# Get all files and extract dates AND hours
files <- list.files(data_path, pattern = "^polygons_.*\\.geojson$", full.names = TRUE)
if (length(files) == 0) stop("No polygon files found")

# Extract dates and hours from filenames (polygons_20251210t142438.geojson)
file_info <- lapply(basename(files), function(f) {
  # Extract YYYYMMDDtHHMMSS from filename
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

# Check which dates already have daily summaries
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
  
  # Create reference hex grid (from first file)
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
  
  # Calculate centroids for each hex (in WGS84)
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
  
  # Process each date
  for (date in dates_to_process) {
    cat(sprintf("  Processing %s...\n", date))
    
    date_files_info <- files_df %>% filter(date == !!date)
    date_files <- date_files_info$file
    cat(sprintf("    %d files\n", length(date_files)))
    
    # Initialize hex grid for this date
    hex_grid <- hex_grid_template
    hex_grid$count <- 0L
    hex_grid$hours_list <- vector("list", nrow(hex_grid))
    hex_grid$datetimes_list <- vector("list", nrow(hex_grid))
    
    # Process all files for this date
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
    
    # Format hours and datetimes as JSON strings
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

    # Save daily summary
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

# Get all daily summaries
all_daily_files <- list.files(
  file.path(output_dir, "daily"),
  pattern = "^daily_.*\\.geojson$",
  full.names = TRUE
)

if (length(all_daily_files) > 0) {
  # Extract year-month from dates
  daily_dates <- sub(".*daily_(\\d{4}-\\d{2})-\\d{2}\\.geojson", "\\1", all_daily_files)
  unique_months <- unique(daily_dates)
  
  cat(sprintf("  Processing %d months...\n", length(unique_months)))
  
  for (month in unique_months) {
    month_files <- all_daily_files[grepl(month, all_daily_files)]
    cat(sprintf("  %s: %d days\n", month, length(month_files)))
    
    # Read and combine all daily summaries for this month
    month_data <- lapply(month_files, function(f) {
      st_read(f, quiet = TRUE)
    })
    
    # Aggregate: sum counts across days, merge hours
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

    # Save monthly summary
    monthly_path <- file.path(output_dir, "monthly", sprintf("monthly_%s.geojson", month))
    st_write(combined, monthly_path, delete_dsn = TRUE, quiet = TRUE)
    
    cat(sprintf("    → %d hexes\n", nrow(combined)))
  }
}

# =================================================================
# STEP 4: Create TOTAL summary (from dailies)
# =================================================================
cat("\n[4] Creating total summary...\n")

if (length(all_daily_files) > 0) {
  # Read ALL daily summaries
  cat("  Reading all daily summaries...\n")
  all_daily_data <- lapply(all_daily_files, function(f) {
    st_read(f, quiet = TRUE)
  })
  
  total_combined <- bind_rows(all_daily_data)

  # Aggregate with hour and day tracking
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
      hours_count = total_count  # This is the number of hourly files the hex appeared in
    ) %>%
    filter(total_count > 0)

  total_combined <- st_make_valid(total_combined)

  cat(sprintf("  %d hexes affected across %d days\n", 
              nrow(total_combined), length(all_daily_files)))
  
  # Save total GeoJSON
  total_geojson <- file.path(output_dir, "total", "total_exposure.geojson")
  st_write(total_combined, total_geojson, delete_dsn = TRUE, quiet = TRUE)
  cat(sprintf("  Saved: %s\n", total_geojson))
  
  # Save total CSV
  total_csv <- file.path(output_dir, "total", "total_stats.csv")
  total_combined %>%
    st_drop_geometry() %>%
    arrange(desc(hours_count)) %>%
    write.csv(total_csv, row.names = FALSE)
  cat(sprintf("  Saved: %s\n", total_csv))
  
  # Save total shapefile
  shp_dir <- file.path(output_dir, "total", "shapefile")
  dir.create(shp_dir, recursive = TRUE, showWarnings = FALSE)
  
  tryCatch({
    st_write(total_combined,
             file.path(shp_dir, "total_exposure.shp"),
             delete_dsn = TRUE, quiet = TRUE)
  }, error = function(e) {
    cat("  Warning: Could not create shapefile\n")
  })
  
  # Summary stats
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
}

# =================================================================
# STEP 5: Create HTML viewer (French, with date/hour selector)
# =================================================================
cat("\n[5] Creating HTML viewer...\n")

html <- sprintf('<!DOCTYPE html>
<html lang="fr">
<head>
    <title>Pannes de courant cumulatives</title>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width,initial-scale=1">
    <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
    <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
    <style>
        body{margin:0;padding:0;font-family:-apple-system,system-ui,sans-serif}
        #map{position:absolute;top:0;bottom:0;width:100%%}
        .info{padding:12px;background:white;box-shadow:0 2px 10px rgba(0,0,0,0.1);border-radius:8px}
        .info h4{margin:0 0 8px;font-size:15px;font-weight:600}
        .info p{margin:4px 0;font-size:13px}
        .legend{line-height:20px;font-size:12px}
        .legend i{width:18px;height:18px;float:left;margin-right:8px;opacity:0.8;border-radius:2px}
        .controls{padding:10px;background:white;box-shadow:0 2px 10px rgba(0,0,0,0.1);border-radius:8px;max-width:250px}
        .controls h4{margin:0 0 8px;font-size:14px;font-weight:600}
        .controls label{display:block;margin:8px 0 4px;font-size:12px;font-weight:500}
        .controls select{width:100%%;padding:6px;border-radius:4px;border:1px solid #ddd;font-size:12px}
        .detail-link{color:#0066cc;cursor:pointer;text-decoration:underline;font-size:12px;margin-top:8px;display:inline-block}
        .detail-link:hover{color:#0052a3}
        .detail-modal{display:none;position:fixed;z-index:2000;left:50%%;top:50%%;transform:translate(-50%%,-50%%);
                      background:white;padding:20px;border-radius:8px;box-shadow:0 4px 20px rgba(0,0,0,0.3);
                      max-width:500px;max-height:70vh;overflow-y:auto}
        .detail-modal.active{display:block}
        .modal-overlay{display:none;position:fixed;z-index:1999;left:0;top:0;width:100%%;height:100%%;
                       background:rgba(0,0,0,0.5)}
        .modal-overlay.active{display:block}
        .close-modal{float:right;font-size:24px;font-weight:bold;cursor:pointer;color:#999}
        .close-modal:hover{color:#333}
        .detail-table{width:100%%;border-collapse:collapse;margin-top:10px}
        .detail-table th,.detail-table td{text-align:left;padding:6px;border-bottom:1px solid #eee;font-size:12px}
        .detail-table th{font-weight:600;background:#f5f5f5}
    </style>
</head>
<body>
    <div id="map"></div>
    <div class="modal-overlay" id="modalOverlay"></div>
    <div class="detail-modal" id="detailModal">
        <span class="close-modal" onclick="closeModal()">&times;</span>
        <div id="modalContent"></div>
    </div>
    <script>
        var map = L.map("map").setView([46.8,-71.2],7);
        L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png",{attribution:"© OSM"}).addTo(map);
        
        var currentLayer;
        var allData = {};
        
        // Load all daily data on startup
        async function loadAllDailyData() {
            try {
                const response = await fetch("total/total_exposure.geojson");
                const data = await response.json();
                allData.total = data;
                
                // Extract unique dates from the data
                const dates = new Set();
                data.features.forEach(f => {
                    if (f.properties.datetimes_affected) {
                        const datetimes = f.properties.datetimes_affected.split(",");
                        datetimes.forEach(dt => {
                            const date = dt.split(" ")[0];
                            dates.add(date);
                        });
                    }
                });
                
                // Populate date selector
                const dateSelect = document.getElementById("dateSelect");
                const sortedDates = Array.from(dates).sort();
                sortedDates.forEach(date => {
                    const opt = document.createElement("option");
                    opt.value = date;
                    opt.text = date;
                    dateSelect.appendChild(opt);
                });
                
            } catch (e) {
                console.error("Error loading data:", e);
            }
        }
        
        function filterDataByDateTime(date, hour) {
            if (!allData.total) return null;
            
            if (date === "all") {
                return allData.total;
            }
            
            const filtered = {
                type: "FeatureCollection",
                features: allData.total.features.filter(f => {
                    if (!f.properties.datetimes_affected) return false;
                    const datetimes = f.properties.datetimes_affected.split(",");
                    return datetimes.some(dt => {
                        if (hour === "all") {
                            return dt.startsWith(date);
                        } else {
                            const targetDateTime = date + " " + hour.padStart(2, "0") + ":00";
                            return dt === targetDateTime;
                        }
                    });
                }).map(f => {
                    // Recalculate count for this filter
                    const datetimes = f.properties.datetimes_affected.split(",");
                    let matchingCount = 0;
                    datetimes.forEach(dt => {
                        if (hour === "all" && dt.startsWith(date)) {
                            matchingCount++;
                        } else if (hour !== "all") {
                            const targetDateTime = date + " " + hour.padStart(2, "0") + ":00";
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
            const date = document.getElementById("dateSelect").value;
            const hour = document.getElementById("hourSelect").value;
            
            const data = filterDataByDateTime(date, hour);
            if (!data) return;
            
            if (currentLayer) map.removeLayer(currentLayer);
            
            const isFiltered = date !== "all";
            
            currentLayer = L.geoJSON(data, {
                style: f => ({
                    fillColor: getColor(isFiltered ? f.properties.filtered_hours_count : f.properties.hours_count),
                    weight: 0.5,
                    color: "#fff",
                    fillOpacity: 0.75
                }),
                onEachFeature: (f, layer) => {
                    const props = f.properties;
                    const hoursCount = isFiltered ? props.filtered_hours_count : props.hours_count;
                    
                    var popup = `<b>Hexagone #${props.hex_id}</b><br>` +
                               `<b>Nombre d\'\'heures impactées:</b> ${hoursCount}<br>` +
                               `<b>Nombre de jours impactés:</b> ${props.days_affected}<br>` +
                               `<b>Coordonnées du centroïde:</b><br>` +
                               `Lat: ${props.centroid_lat.toFixed(6)}, Lon: ${props.centroid_lon.toFixed(6)}<br>` +
                               `<a class="detail-link" onclick="showDetails(${props.hex_id})">Voir toutes les heures et jours impactés</a>`;
                    
                    layer.bindPopup(popup);
                }
            }).addTo(map);
        }
        
        function showDetails(hexId) {
            const feature = allData.total.features.find(f => f.properties.hex_id === hexId);
            if (!feature) return;
            
            const props = feature.properties;
            const datetimes = props.datetimes_affected.split(",").sort();
            
            // Group by date
            const byDate = {};
            datetimes.forEach(dt => {
                const [date, time] = dt.split(" ");
                if (!byDate[date]) byDate[date] = [];
                byDate[date].push(time);
            });
            
            let html = `<h3>Hexagone #${hexId}</h3>`;
            html += `<p><b>Total heures impactées:</b> ${props.hours_count}</p>`;
            html += `<p><b>Total jours impactés:</b> ${props.days_affected}</p>`;
            html += `<p><b>Centroïde:</b> ${props.centroid_lat.toFixed(6)}, ${props.centroid_lon.toFixed(6)}</p>`;
            html += `<table class="detail-table">`;
            html += `<thead><tr><th>Date</th><th>Heures</th></tr></thead><tbody>`;
            
            Object.keys(byDate).sort().forEach(date => {
                const hours = byDate[date].sort();
                html += `<tr><td>${date}</td><td>${hours.join(", ")}</td></tr>`;
            });
            
            html += `</tbody></table>`;
            
            document.getElementById("modalContent").innerHTML = html;
            document.getElementById("detailModal").classList.add("active");
            document.getElementById("modalOverlay").classList.add("active");
        }
        
        function closeModal() {
            document.getElementById("detailModal").classList.remove("active");
            document.getElementById("modalOverlay").classList.remove("active");
        }
        
        document.getElementById("modalOverlay").onclick = closeModal;
        
        function getColor(d) {
            return d>80?"#67000d":d>60?"#a50f15":d>40?"#cb181d":d>20?"#ef3b2c":
                   d>10?"#fb6a4a":d>5?"#fc9272":d>2?"#fcbba1":"#fee5d9";
        }
        
        // Initialize
        loadAllDailyData().then(() => {
            updateMap();
        });
        
        var legend = L.control({position:"bottomright"});
        legend.onAdd = () => {
            var div = L.DomUtil.create("div","info legend");
            div.innerHTML = "<h4>Heures (nombre)</h4>";
            [0,2,5,10,20,40,60,80].forEach((g,i,a) => {
                div.innerHTML += `<i style="background:${getColor(g+1)}"></i>${g}${a[i+1]?"–"+a[i+1]:"+"}<br>`;
            });
            return div;
        };
        legend.addTo(map);
        
        var info = L.control({position:"topright"});
        info.onAdd = () => {
            var div = L.DomUtil.create("div","info");
            div.innerHTML = "<h4>Pannes de courant cumulatives</h4>" +
                           "<p><b>Taille des hexagones:</b> %d km</p>" +
                           "<p><b>Jours analysés:</b> %d</p>";
            return div;
        };
        info.addTo(map);
        
        var controls = L.control({position:"topleft"});
        controls.onAdd = () => {
            var div = L.DomUtil.create("div","controls");
            div.innerHTML = "<h4>Filtres</h4>" +
                           "<label>Date:</label>" +
                           "<select id=\\"dateSelect\\" onchange=\\"updateMap()\\"><option value=\\"all\\">Résumé complet (défaut)</option></select>" +
                           "<label>Heure:</label>" +
                           "<select id=\\"hourSelect\\" onchange=\\"updateMap()\\"><option value=\\"all\\">Toutes</option>" +
                           Array.from({length: 24}, (_, i) => `<option value=\\"${i}\\">${i}:00</option>`).join("") +
                           "</select>";
            return div;
        };
        controls.addTo(map);
    </script>
</body>
</html>', HEX_SIZE/1000, length(all_daily_files))

writeLines(html, file.path(output_dir, "index.html"))

# =================================================================
# Done!
# =================================================================
elapsed <- as.numeric(difftime(Sys.time(), start_time, units = "mins"))
cat(sprintf("\n✅ COMPLETE in %.1f minutes\n", elapsed))
cat("\nOutput structure:\n")
cat("  public/\n")
cat("    daily/     - Daily summaries (one per date)\n")
cat("    monthly/   - Monthly aggregations\n")
cat("    total/     - Cumulative summary of all data\n")
cat("    index.html - Interactive map\n")

# Verify critical files exist
cat("\nVerifying files...\n")
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
  cat("\n❌ ERROR: Some required files were not created!\n")
  quit(status = 1)
}

cat("\n✅ All required files verified\n")
