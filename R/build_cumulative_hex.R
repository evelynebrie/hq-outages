# HQ Outages - OPTIMIZED with Daily & Monthly Summaries
# Works directly with GeoJSON polygons (no CSV customer data required)
# Creates persistent summaries for faster future runs

suppressPackageStartupMessages({
  library(sf)
  library(dplyr)
  library(jsonlite)
  library(data.table)
  library(lubridate)
})

cat("=== HQ Outages with Daily & Monthly Summaries ===\n")
cat("Strategy: Build daily summaries incrementally, aggregate to monthly, then to total\n")
cat(sprintf("\nStarting at: %s\n", format(Sys.time(), "%Y-%m-%d %H:%M:%S")))
start_time <- Sys.time()

# Config
data_path <- "data/daily"  # Read flattened files
output_dir <- "public"
daily_summaries_dir <- file.path(output_dir, "daily_summaries")
monthly_summaries_dir <- file.path(output_dir, "monthly_summaries")

HEX_SIZE <- 1000
SIMPLIFY <- 200
ANALYSIS_START_DATE <- as.Date("2026-01-01")  # Start analysis from this date

# Create directories
dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(daily_summaries_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(monthly_summaries_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(file.path(output_dir, "total"), recursive = TRUE, showWarnings = FALSE)

# =================================================================
# HELPER FUNCTIONS
# =================================================================

# Find all GeoJSON files in the hourly data structure
find_polygon_files <- function(data_path, start_date = NULL) {
  cat("\n[Finding polygon files]\n")
  
  # Find all date directories
  date_dirs <- list.dirs(data_path, recursive = FALSE, full.names = TRUE)
  date_dirs <- date_dirs[grepl("date=\\d{4}-\\d{2}-\\d{2}", basename(date_dirs))]
  
  files_list <- list()
  
  for (date_dir in date_dirs) {
    date_str <- sub("date=", "", basename(date_dir))
    date_val <- as.Date(date_str)
    
    # Skip dates before analysis start
    if (!is.null(start_date) && date_val < start_date) {
      next
    }
    
    # Find hour directories
    hour_dirs <- list.dirs(date_dir, recursive = FALSE, full.names = TRUE)
    hour_dirs <- hour_dirs[grepl("hour=\\d{1,2}", basename(hour_dirs))]
    
    for (hour_dir in hour_dirs) {
      hour_str <- sub("hour=", "", basename(hour_dir))
      hour_val <- as.integer(hour_str)
      
      # Find polygon files in this hour
      files <- list.files(hour_dir, pattern = "^polygons_.*\\.geojson$", full.names = TRUE)
      
      if (length(files) > 0) {
        # Take the latest file if multiple exist (based on timestamp in filename)
        if (length(files) > 1) {
          timestamps <- regmatches(basename(files), 
                                   regexpr("\\d{8}T\\d{6}", basename(files), ignore.case = TRUE))
          files <- files[which.max(timestamps)]
        }
        
        files_list[[length(files_list) + 1]] <- data.table(
          file = files[1],
          date = date_str,
          hour = hour_val,
          datetime = sprintf("%s %02d:00:00", date_str, hour_val)
        )
      }
    }
  }
  
  if (length(files_list) == 0) {
    stop("No polygon files found in ", data_path)
  }
  
  files_dt <- rbindlist(files_list)
  setorder(files_dt, date, hour)
  
  cat(sprintf("  Found %d hourly snapshots\n", nrow(files_dt)))
  cat(sprintf("  Date range: %s to %s\n", min(files_dt$date), max(files_dt$date)))
  cat(sprintf("  Total unique dates: %d\n", length(unique(files_dt$date))))
  
  return(files_dt)
}

# Load or create hex grid
get_or_create_hex_grid <- function(output_dir, bbox = NULL) {
  hex_grid_path <- file.path(output_dir, "hex_grid_template.rds")
  
  if (file.exists(hex_grid_path)) {
    cat("  Loading existing hex grid...\n")
    hex_grid <- readRDS(hex_grid_path)
  } else {
    cat("  Creating new hex grid...\n")
    
    if (is.null(bbox)) {
      stop("Cannot create hex grid without bounding box")
    }
    
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
  
  return(hex_grid)
}

# Process a single day's data and create daily summary
process_daily_summary <- function(date_str, files_dt, hex_grid, daily_summaries_dir) {
  daily_summary_path <- file.path(daily_summaries_dir, sprintf("daily_%s.rds", date_str))
  
  # Check if already processed
  if (file.exists(daily_summary_path)) {
    cat(sprintf("  [%s] Loading existing summary\n", date_str))
    return(readRDS(daily_summary_path))
  }
  
  cat(sprintf("  [%s] Processing new summary\n", date_str))
  
  # Get files for this date
  date_files <- files_dt[date == date_str]
  
  if (nrow(date_files) == 0) {
    return(NULL)
  }
  
  # Load all polygons for this date
  polys_list <- list()
  for (i in seq_len(nrow(date_files))) {
    tryCatch({
      poly <- st_read(date_files$file[i], quiet = TRUE) %>%
        st_transform(32618) %>%
        st_simplify(dTolerance = SIMPLIFY, preserveTopology = FALSE)
      
      if (nrow(poly) > 1) {
        poly <- st_union(poly) %>% st_sf()
      }
      
      poly$datetime <- date_files$datetime[i]
      poly$hour <- date_files$hour[i]
      
      polys_list[[length(polys_list) + 1]] <- poly
    }, error = function(e) {
      cat(sprintf("    Warning: Failed to load %s\n", basename(date_files$file[i])))
    })
  }
  
  if (length(polys_list) == 0) {
    return(NULL)
  }
  
  # Combine polygons
  polys_list <- lapply(polys_list, function(poly) {
    st_sf(
      datetime = poly$datetime,
      hour = poly$hour,
      geometry = st_geometry(poly)
    )
  })
  
  all_polys <- do.call(rbind, polys_list)
  
  # Find intersections
  intersections <- st_intersects(hex_grid, all_polys, sparse = TRUE)
  
  # Build daily results
  daily_results <- data.table(
    hex_id = integer(),
    hours_count = integer(),
    hours_list = character(),
    datetimes_list = character()
  )
  
  for (i in seq_len(nrow(hex_grid))) {
    poly_indices <- intersections[[i]]
    
    if (length(poly_indices) > 0) {
      affected_datetimes <- all_polys$datetime[poly_indices]
      affected_hours <- all_polys$hour[poly_indices]
      
      daily_results <- rbind(daily_results, data.table(
        hex_id = hex_grid$hex_id[i],
        hours_count = length(unique(affected_hours)),  # Count unique hours
        hours_list = paste(sort(unique(affected_hours)), collapse = ","),
        datetimes_list = paste(sort(affected_datetimes), collapse = ",")
      ))
    }
  }
  
  if (nrow(daily_results) > 0) {
    daily_results$date <- date_str
    saveRDS(daily_results, daily_summary_path)
  }
  
  return(daily_results)
}

# Aggregate daily summaries into monthly summary
process_monthly_summary <- function(year, month, daily_summaries_dir, monthly_summaries_dir) {
  monthly_summary_path <- file.path(monthly_summaries_dir, 
                                     sprintf("monthly_%04d-%02d.rds", year, month))
  
  # Check if already processed
  if (file.exists(monthly_summary_path)) {
    cat(sprintf("  [%04d-%02d] Loading existing monthly summary\n", year, month))
    return(readRDS(monthly_summary_path))
  }
  
  cat(sprintf("  [%04d-%02d] Creating new monthly summary\n", year, month))
  
  # Find all daily summaries for this month
  pattern <- sprintf("daily_%04d-%02d-.*\\.rds", year, month)
  daily_files <- list.files(daily_summaries_dir, pattern = pattern, full.names = TRUE)
  
  if (length(daily_files) == 0) {
    return(NULL)
  }
  
  # Load and combine all daily summaries
  all_dailies <- lapply(daily_files, readRDS)
  all_dailies <- rbindlist(all_dailies)
  
  # Aggregate by hex_id
  monthly_results <- all_dailies[, .(
    total_hours = sum(hours_count),
    days_affected = .N,
    dates_affected = paste(sort(unique(date)), collapse = ","),
    all_datetimes = paste(unique(unlist(strsplit(datetimes_list, ","))), collapse = ",")
  ), by = hex_id]
  
  monthly_results$year <- year
  monthly_results$month <- month
  
  saveRDS(monthly_results, monthly_summary_path)
  
  return(monthly_results)
}

# =================================================================
# MAIN PROCESSING
# =================================================================

# Step 1: Find all polygon files
cat("\n[STEP 1: Finding polygon files]\n")
files_dt <- find_polygon_files(data_path, start_date = ANALYSIS_START_DATE)

# Step 2: Create or load hex grid
cat("\n[STEP 2: Setting up hex grid]\n")

# First, we need to get bounding box from a sample of polygons
sample_files <- head(files_dt$file, min(10, nrow(files_dt)))
sample_polys <- lapply(sample_files, function(f) {
  st_read(f, quiet = TRUE) %>% st_transform(32618)
}) %>% do.call(rbind, .)

bbox <- st_bbox(sample_polys)
bbox["xmin"] <- bbox["xmin"] - 50000
bbox["xmax"] <- bbox["xmax"] + 50000
bbox["ymin"] <- bbox["ymin"] - 50000
bbox["ymax"] <- bbox["ymax"] + 50000
class(bbox) <- "bbox"
attr(bbox, "crs") <- st_crs(32618)

hex_grid <- get_or_create_hex_grid(output_dir, bbox)
cat(sprintf("  Grid size: %d hexagons\n", nrow(hex_grid)))

# Step 3: Process daily summaries
cat("\n[STEP 3: Processing daily summaries]\n")
unique_dates <- unique(files_dt$date)
cat(sprintf("  Processing %d unique dates\n", length(unique_dates)))

daily_summaries <- list()
for (date_str in unique_dates) {
  daily_result <- process_daily_summary(date_str, files_dt, hex_grid, daily_summaries_dir)
  if (!is.null(daily_result)) {
    daily_summaries[[date_str]] <- daily_result
  }
}

# Step 4: Process monthly summaries
cat("\n[STEP 4: Processing monthly summaries]\n")
dates <- as.Date(unique_dates)
year_months <- unique(format(dates, "%Y-%m"))
cat(sprintf("  Processing %d unique months\n", length(year_months)))

monthly_summaries <- list()
for (ym in year_months) {
  parts <- strsplit(ym, "-")[[1]]
  year <- as.integer(parts[1])
  month <- as.integer(parts[2])
  
  monthly_result <- process_monthly_summary(year, month, daily_summaries_dir, monthly_summaries_dir)
  if (!is.null(monthly_result)) {
    monthly_summaries[[ym]] <- monthly_result
  }
}

# Step 5: Create total cumulative from monthly summaries
cat("\n[STEP 5: Creating total cumulative]\n")

# Combine all monthly summaries
all_monthly <- rbindlist(monthly_summaries)

# Aggregate to total
total_results <- all_monthly[, .(
  hours_count = sum(total_hours),
  days_affected = length(unique(unlist(strsplit(dates_affected, ",")))),
  datetimes_affected = paste(unique(unlist(strsplit(all_datetimes, ","))), collapse = ",")
), by = hex_id]

# Calculate number of times each datetime appears (for proper hour counting)
total_results[, hours_count := sapply(datetimes_affected, function(dt_str) {
  if (is.na(dt_str) || dt_str == "") return(0L)
  length(strsplit(dt_str, ",")[[1]])
})]

cat(sprintf("  Total hexagons affected: %d\n", nrow(total_results)))

# Join with hex grid geometry
hex_results_sf <- hex_grid %>%
  inner_join(total_results, by = "hex_id") %>%
  st_transform(4326) %>%
  st_make_valid()

# Step 6: Save total cumulative
cat("\n[STEP 6: Saving total cumulative]\n")

current_date <- format(Sys.time(), "%Y-%m-%d")
total_path <- file.path(output_dir, "total", "total_exposure.geojson")

st_write(hex_results_sf, total_path, delete_dsn = TRUE, quiet = TRUE)
cat(sprintf("  ✓ Total exposure saved: %d hexagons\n", nrow(hex_results_sf)))

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
  analysis_start_date = as.character(ANALYSIS_START_DATE),
  total_hexes_affected = nrow(hex_results_sf),
  total_hours_tracked = sum(hex_results_sf$hours_count),
  date_range = list(
    first = min(files_dt$date),
    last = max(files_dt$date)
  ),
  hex_size_meters = HEX_SIZE,
  processing_info = list(
    method = "incremental daily/monthly summaries",
    total_polygon_files = nrow(files_dt),
    daily_summaries = length(daily_summaries),
    monthly_summaries = length(monthly_summaries)
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

# Step 7: Generate HTML
cat("\n[STEP 7: Generating HTML map]\n")

num_days <- length(unique(files_dt$date))
hex_size_km <- round(HEX_SIZE / 1000, 1)

# Generate HTML with detail modal and correct hour counting
hour_options <- paste(sapply(0:23, function(i) sprintf('<option value="%d">%d:00</option>', i, i)), collapse="")

html_template <- sprintf('<!DOCTYPE html>
<html lang="fr">
<head>
    <title>Pannes de courant cumulatives - Hydro-Québec</title>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width,initial-scale=1">
    <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
    <link rel="stylesheet" href="https://unpkg.com/leaflet-control-geocoder/dist/Control.Geocoder.css"/>
    <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
    <script src="https://unpkg.com/leaflet-control-geocoder/dist/Control.Geocoder.js"></script>
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
                      max-width:90vw;width:600px;max-height:85vh;overflow-y:auto}
        .detail-modal.active{display:block}
        .modal-overlay{display:none;position:fixed;z-index:1999;left:0;top:0;width:100%%;height:100%%;
                       background:rgba(0,0,0,0.5)}
        .modal-overlay.active{display:block}
        .close-modal{float:right;font-size:24px;font-weight:bold;cursor:pointer;color:#999;line-height:1}
        .close-modal:hover{color:#333}
        .detail-table{width:100%%;border-collapse:collapse;margin-top:10px;font-size:13px}
        .detail-table th,.detail-table td{text-align:left;padding:8px;border-bottom:1px solid #eee}
        .detail-table th{font-weight:600;background:#f5f5f5;position:sticky;top:0}
        .detail-table tfoot td{font-weight:600;background:#f9f9f9;border-top:2px solid #ccc}
        @media (max-width: 768px) {
            .detail-modal{width:95vw;max-height:90vh;padding:15px}
            .detail-table{font-size:12px}
            .detail-table th,.detail-table td{padding:6px}
        }
    </style>
</head>
<body>
    <div id="map"></div>
    <div class="modal-overlay" id="modalOverlay"></div>
    <div class="detail-modal" id="detailModal">
        <span class="close-modal" onclick="closeDetailModal()">&times;</span>
        <div id="modalContent"></div>
    </div>
    <script>
        var map = L.map("map").setView([46.2, -72.5], 9);
        L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png",{attribution:"© OSM"}).addTo(map);
        var currentLayer,allData={},legendControl=null;
        function getColor(d){return d>80?"#67000d":d>60?"#a50f15":d>40?"#cb181d":d>20?"#ef3b2c":d>10?"#fb6a4a":d>5?"#fc9272":d>2?"#fcbba1":"#fee5d9";}
        async function loadData(){
            try{
                const r=await fetch("total/total_exposure.geojson");
                const data=await r.json();
                allData.total=data;
                const dates=new Set();
                data.features.forEach(f=>{
                    if(f.properties.datetimes_affected){
                        f.properties.datetimes_affected.split(",").forEach(dt=>{
                            const c=dt.trim();
                            if(c){const d=c.split(" ")[0];dates.add(d);}
                        });
                    }
                });
                const sel=document.getElementById("dateSelect");
                Array.from(dates).sort().forEach(d=>{const o=document.createElement("option");o.value=d;o.text=d;sel.appendChild(o);});
                updateMap();
            }catch(e){console.error("Error:",e);alert("Erreur de chargement");}
        }
        function updateMap(){
            const df=document.getElementById("dateSelect").value;
            const hf=document.getElementById("hourSelect").value;
            if(currentLayer)map.removeLayer(currentLayer);
            const isFilt=df!=="all"||hf!=="all";
            currentLayer=L.geoJSON(allData.total,{
                filter:f=>{
                    if(!isFilt)return true;
                    if(!f.properties.datetimes_affected)return false;
                    return f.properties.datetimes_affected.split(",").map(s=>s.trim()).some(dt=>{
                        if(!dt)return false;
                        const p=dt.split(" ");
                        if(p.length!==2)return false;
                        const d=p[0],t=p[1],h=parseInt(t.split(":")[0]);
                        return(df==="all"||d===df)&&(hf==="all"||h===parseInt(hf));
                    });
                },
                style:f=>({fillColor:getColor(f.properties.hours_count),weight:1,opacity:0.6,color:"#666",fillOpacity:0.7}),
                onEachFeature:(f,layer)=>{
                    const p=f.properties;
                    let dh=p.hours_count;
                    if(isFilt&&p.datetimes_affected){
                        const fdt=p.datetimes_affected.split(",").map(s=>s.trim()).filter(s=>s).filter(dt=>{
                            const parts=dt.split(" ");
                            if(parts.length!==2)return false;
                            const d=parts[0],t=parts[1],h=parseInt(t.split(":")[0]);
                            return(df==="all"||d===df)&&(hf==="all"||h===parseInt(hf));
                        });
                        dh=fdt.length;
                    }
                    const popup=`<div style="min-width:200px"><b>Hexagone #${p.hex_id}</b><br><b>Heures affectées:</b> ${dh}<br><b>Jours affectés:</b> ${p.days_affected}<br><b>Centroïde:</b> ${p.centroid_lat.toFixed(6)}, ${p.centroid_lon.toFixed(6)}<br><a class="detail-link" onclick="showDetails(${p.hex_id})">Voir les détails →</a></div>`;
                    layer.bindPopup(popup);
                }
            }).addTo(map);
            if(legendControl){if(isFilt){map.removeControl(legendControl);}else if(!map.hasControl(legendControl)){legendControl.addTo(map);}}
        }
        function showDetails(hexId){
            const f=allData.total.features.find(f=>f.properties.hex_id===hexId);
            if(!f){alert("Données non trouvées");return;}
            const p=f.properties;
            if(!p.datetimes_affected){alert("Aucune donnée temporelle");return;}
            const dts=p.datetimes_affected.split(",").map(s=>s.trim()).filter(s=>s).sort();
            if(dts.length===0){alert("Aucune donnée temporelle");return;}
            const byDate={};
            dts.forEach(dt=>{
                const parts=dt.split(" ");
                if(parts.length!==2)return;
                const d=parts[0],t=parts[1];
                if(!byDate[d])byDate[d]=[];
                const hm=t.match(/^(\\d{1,2}):/);
                if(hm){const h=parseInt(hm[1]);byDate[d].push(h);}
            });
            let html="<h3>Hexagone #"+hexId+"</h3>";
            html+="<p><strong>Centroïde:</strong> "+p.centroid_lat.toFixed(6)+", "+p.centroid_lon.toFixed(6)+"</p>";
            html+="<h4>Heures affectées par date:</h4>";
            html+="<table class=\\"detail-table\\"><thead><tr><th>Date</th><th>Heures affectées</th><th>Total</th></tr></thead><tbody>";
            const sdates=Object.keys(byDate).sort();
            let th=0;
            sdates.forEach(d=>{
                const hs=byDate[d].sort((a,b)=>a-b);
                const hl=hs.map(h=>h+":00").join(", ");
                const c=hs.length;
                th+=c;
                html+="<tr><td>"+d+"</td><td>"+hl+"</td><td><b>"+c+"</b></td></tr>";
            });
            html+="</tbody><tfoot><tr><td colspan=\\"2\\"><strong>Total</strong></td><td><strong>"+th+" heures sur "+sdates.length+" jours</strong></td></tr></tfoot></table>";
            html+="<p style=\\"font-size:11px;color:#666;margin-top:10px;\\"><em>Chaque entrée représente une heure où ce secteur était marqué comme ayant une panne</em></p>";
            document.getElementById("modalContent").innerHTML=html;
            document.getElementById("detailModal").classList.add("active");
            document.getElementById("modalOverlay").classList.add("active");
        }
        function closeDetailModal(){
            document.getElementById("detailModal").classList.remove("active");
            document.getElementById("modalOverlay").classList.remove("active");
        }
        document.getElementById("modalOverlay").onclick=closeDetailModal;
        loadData();
        legendControl=L.control({position:"bottomright"});
        legendControl.onAdd=()=>{
            var div=L.DomUtil.create("div","info legend");
            div.innerHTML="<h4>Heures affectées</h4>";
            [0,2,5,10,20,40,60,80].forEach((g,i,a)=>{
                div.innerHTML+="<i style=\\"background:"+getColor(g+1)+"\\"></i>"+g+(a[i+1]?"–"+a[i+1]:"+")+  "<br>";
            });
            return div;
        };
        legendControl.addTo(map);
        var geocoder=L.Control.geocoder({defaultMarkGeocode:false,placeholder:"Adresse ou code postal...",errorMessage:"Adresse non trouvée"})
        .on("markgeocode",function(e){
            var bbox=e.geocode.bbox;
            var poly=L.polygon([bbox.getSouthEast(),bbox.getNorthEast(),bbox.getNorthWest(),bbox.getSouthWest()]);
            map.fitBounds(poly.getBounds());
        }).addTo(map);
        var info=L.control({position:"topright"});
        info.onAdd=()=>{
            var div=L.DomUtil.create("div","info");
            div.innerHTML="<h4>Pannes cumulatives</h4><p><b>Hexagones:</b> %s km</p><p><b>Jours:</b> %d</p><p><b>Début analyse:</b> %s</p>";
            return div;
        };
        info.addTo(map);
        var controls=L.control({position:"topleft"});
        controls.onAdd=()=>{
            var div=L.DomUtil.create("div","controls");
            div.innerHTML="<h4>Filtres</h4><label>Date:</label><select id=\\"dateSelect\\" onchange=\\"updateMap()\\"><option value=\\"all\\">Toutes (défaut)</option></select><label>Heure:</label><select id=\\"hourSelect\\" onchange=\\"updateMap()\\"><option value=\\"all\\">Toutes</option>%s</select>";
            return div;
        };
        controls.addTo(map);
    </script>
</body>
</html>', hex_size_km, num_days, as.character(ANALYSIS_START_DATE), hour_options)

html_path <- file.path(output_dir, "index.html")
writeLines(html_template, html_path)
cat(sprintf("  ✓ HTML saved: %s\n", html_path))

# Summary
elapsed <- as.numeric(difftime(Sys.time(), start_time, units = "secs"))
cat(sprintf("\n✅ PROCESSING COMPLETE in %.1f seconds (%.1f minutes)\n", 
            elapsed, elapsed/60))

cat("\n📊 PROCESSING SUMMARY:\n")
cat(sprintf("  • Analysis start date: %s\n", ANALYSIS_START_DATE))
cat(sprintf("  • Total polygon files: %d\n", nrow(files_dt)))
cat(sprintf("  • Daily summaries: %d\n", length(daily_summaries)))
cat(sprintf("  • Monthly summaries: %d\n", length(monthly_summaries)))
cat(sprintf("  • Hexagons affected: %d\n", nrow(hex_results_sf)))
cat(sprintf("  • Processing time: %.1f minutes\n", elapsed/60))

cat("\n✅ READY FOR DEPLOYMENT\n")
