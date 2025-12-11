# HQ Outages - HIERARCHICAL AGGREGATION SYSTEM
# Processes daily → monthly → total summaries

suppressPackageStartupMessages({
  library(sf)
  library(dplyr)
  library(jsonlite)
})

cat("=== HQ Outages Hierarchical Analysis ===\n")
start_time <- Sys.time()

# Config
data_path <- "data/daily"
output_dir <- "public"
HEX_SIZE <- 5000  # 5km hexes
SIMPLIFY <- 200   # 200m simplification

dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(file.path(output_dir, "daily"), recursive = TRUE, showWarnings = FALSE)
dir.create(file.path(output_dir, "monthly"), recursive = TRUE, showWarnings = FALSE)
dir.create(file.path(output_dir, "total"), recursive = TRUE, showWarnings = FALSE)

# =================================================================
# STEP 1: Identify which dates need processing
# =================================================================
cat("\n[1] Identifying dates to process...\n")

# Get all files and extract dates
files <- list.files(data_path, pattern = "^polygons_.*\\.geojson$", full.names = TRUE)
if (length(files) == 0) stop("No polygon files found")

# Extract dates from filenames (polygons_20251210t142438.geojson -> 2025-12-10)
file_dates <- sapply(basename(files), function(f) {
  # Extract YYYYMMDD from filename
  m <- regmatches(f, regexpr("\\d{8}", f))
  if (length(m) > 0) {
    d <- m[1]
    sprintf("%s-%s-%s", substr(d,1,4), substr(d,5,6), substr(d,7,8))
  } else NA
})

files_df <- data.frame(
  file = files,
  date = file_dates,
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
  
  rm(first_data, bbox)
  gc()
  
  cat(sprintf("  Template: %d hexagons\n", nrow(hex_grid_template)))
  
  # Process each date
  for (date in dates_to_process) {
    cat(sprintf("  Processing %s...\n", date))
    
    date_files <- files_df$file[files_df$date == date]
    cat(sprintf("    %d files\n", length(date_files)))
    
    # Initialize hex grid for this date
    hex_grid <- hex_grid_template
    hex_grid$count <- 0L
    
    # Process all files for this date
    for (f in date_files) {
      tryCatch({
        polys <- st_read(f, quiet = TRUE) %>%
          st_transform(32618) %>%
          st_simplify(dTolerance = SIMPLIFY, preserveTopology = FALSE) %>%
          st_union()
        
        hits <- st_intersects(hex_grid, polys, sparse = TRUE)
        affected <- which(lengths(hits) > 0)
        hex_grid$count[affected] <- hex_grid$count[affected] + 1L
        
        rm(polys, hits, affected)
      }, error = function(e) invisible(NULL))
    }
    
    # Calculate percentage and filter
    hex_grid_filtered <- hex_grid %>%
      filter(count > 0) %>%
      mutate(
        date = date,
        n_files = length(date_files),
        pct = round(100 * count / length(date_files), 2)
      ) %>%
      st_transform(4326)

     hex_grid_filtered <- st_make_valid(hex_grid_filtered)

    
    # Save daily summary
    daily_path <- file.path(output_dir, "daily", sprintf("daily_%s.geojson", date))
    st_write(hex_grid_filtered, daily_path, delete_dsn = TRUE, quiet = TRUE)
    
    cat(sprintf("    → %d hexes affected (%.1f%% avg)\n", 
                nrow(hex_grid_filtered), mean(hex_grid_filtered$pct)))
    
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
    
    # Aggregate: sum counts across days
    combined <- bind_rows(month_data) %>%
      group_by(hex_id) %>%
      summarise(
        count = sum(count),
        n_files = sum(n_files),
        .groups = "drop"
      ) %>%
      mutate(
        month = month,
        pct = round(100 * count / n_files, 2)
      ) %>%
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
  
  # Aggregate across ALL days
  total_combined <- bind_rows(all_daily_data) %>%
    group_by(hex_id) %>%
    summarise(
      total_count = sum(count),
      total_files = sum(n_files),
      days_affected = n(),
      .groups = "drop"
    ) %>%
    mutate(
      pct = round(100 * total_count / total_files, 2)
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
    arrange(desc(pct)) %>%
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
    total_files = sum(total_combined$total_files[1]), # Should be same for all hexes
    total_hexes_affected = nrow(total_combined),
    mean_exposure_pct = round(mean(total_combined$pct), 2),
    max_exposure_pct = round(max(total_combined$pct), 2),
    hex_size_m = HEX_SIZE,
    last_updated = as.character(Sys.Date()),
    processing_time_min = round(as.numeric(difftime(Sys.time(), start_time, units = "mins")), 1)
  )
  
  write_json(summary_stats, 
             file.path(output_dir, "total", "summary.json"),
             pretty = TRUE, auto_unbox = TRUE)
  
  cat(sprintf("  Mean exposure: %.1f%%\n", summary_stats$mean_exposure_pct))
  cat(sprintf("  Max exposure: %.1f%%\n", summary_stats$max_exposure_pct))
}

# =================================================================
# STEP 5: Create HTML viewer
# =================================================================
cat("\n[5] Creating HTML viewer...\n")

html <- sprintf('<!DOCTYPE html>
<html>
<head>
    <title>HQ Outage Exposure Analysis</title>
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
        .controls{padding:10px;background:white;box-shadow:0 2px 10px rgba(0,0,0,0.1);border-radius:8px}
        .controls h4{margin:0 0 8px;font-size:14px;font-weight:600}
        .controls select{width:100%%;padding:6px;border-radius:4px;border:1px solid #ddd}
        .downloads a{display:block;margin:4px 0;padding:4px 8px;color:#0066cc;text-decoration:none;border-radius:4px}
        .downloads a:hover{background:#f0f0f0}
    </style>
</head>
<body>
    <div id="map"></div>
    <script>
        var map = L.map("map").setView([46.8,-71.2],7);
        L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png",{attribution:"© OSM"}).addTo(map);
        
        var currentLayer;
        
        function loadData(url, type) {
            if (currentLayer) map.removeLayer(currentLayer);
            
            fetch(url)
                .then(r => r.json())
                .then(data => {
                    currentLayer = L.geoJSON(data, {
                        style: f => ({
                            fillColor: getColor(f.properties.pct),
                            weight: 0.5,
                            color: "#fff",
                            fillOpacity: 0.75
                        }),
                        onEachFeature: (f, layer) => {
                            var popup = `<b>Hex ${f.properties.hex_id}</b><br>Exposure: ${f.properties.pct}%%`;
                            if (type === "total") {
                                popup += `<br>Days affected: ${f.properties.days_affected}`;
                            }
                            layer.bindPopup(popup);
                        }
                    }).addTo(map);
                });
        }
        
        function getColor(d) {
            return d>80?"#67000d":d>60?"#a50f15":d>40?"#cb181d":d>20?"#ef3b2c":
                   d>10?"#fb6a4a":d>5?"#fc9272":d>2?"#fcbba1":"#fee5d9";
        }
        
        // Load total by default
        loadData("total/total_exposure.geojson", "total");
        
        var legend = L.control({position:"bottomright"});
        legend.onAdd = () => {
            var div = L.DomUtil.create("div","info legend");
            div.innerHTML = "<h4>Exposure (%%)</h4>";
            [0,2,5,10,20,40,60,80].forEach((g,i,a) => {
                div.innerHTML += `<i style="background:${getColor(g+1)}"></i>${g}${a[i+1]?"–"+a[i+1]:"+"}<br>`;
            });
            return div;
        };
        legend.addTo(map);
        
        var info = L.control({position:"topright"});
        info.onAdd = () => {
            var div = L.DomUtil.create("div","info");
            div.innerHTML = "<h4>HQ Cumulative Outage Exposure</h4>" +
                           "<p><b>Hex size:</b> %dkm</p>" +
                           "<p><b>Days analyzed:</b> %d</p>";
            return div;
        };
        info.addTo(map);
        
        var controls = L.control({position:"topleft"});
        controls.onAdd = () => {
            var div = L.DomUtil.create("div","controls downloads");
            div.innerHTML = "<h4>📥 Downloads</h4>" +
                           "<a href=\\"total/total_exposure.geojson\\">Total GeoJSON</a>" +
                           "<a href=\\"total/total_stats.csv\\">Total CSV</a>" +
                           "<a href=\\"total/summary.json\\">Summary JSON</a>";
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
