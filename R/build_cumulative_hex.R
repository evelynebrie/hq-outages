# HQ Outages Hexagonal Grid Analysis - GitHub Actions Version
# Processes GeoJSON files and creates hex grid with cumulative exposure scores

library(sf)
library(dplyr)
library(purrr)
library(fs)

cat("=== Starting Hexagonal Grid Analysis ===\n")

# Configuration
data_path <- "data/daily"  # Where GitHub Actions downloads the files
output_dir <- "public"     # Where we'll save outputs for gh-pages
hex_size <- 1000          # Hex size in meters

# Create output directory
dir_create(output_dir)

# Find all GeoJSON files
cat("Searching for GeoJSON files in:", data_path, "\n")
geojson_files <- dir_ls(data_path, regexp = "\\.geojson$")
cat(sprintf("Found %d GeoJSON files\n", length(geojson_files)))

if (length(geojson_files) == 0) {
  stop("No GeoJSON files found in data/daily directory")
}

# Read all GeoJSON files
cat("Reading GeoJSON files...\n")
outage_data <- map(geojson_files, function(file) {
  tryCatch({
    sf::st_read(file, quiet = TRUE)
  }, error = function(e) {
    warning(sprintf("Error reading %s: %s", file, e$message))
    return(NULL)
  })
})

# Remove NULL entries (failed reads)
outage_data <- compact(outage_data)
cat(sprintf("Successfully read %d files\n", length(outage_data)))

if (length(outage_data) == 0) {
  stop("No files could be read successfully")
}

# Combine all polygons
cat("Combining polygons...\n")
all_polygons <- bind_rows(outage_data)

# Ensure CRS is set (WGS84)
if (is.na(st_crs(all_polygons))) {
  all_polygons <- st_set_crs(all_polygons, 4326)
}

# Transform to projected CRS (UTM Zone 18N for Quebec)
cat("Transforming to projected CRS...\n")
all_polygons_proj <- st_transform(all_polygons, 32618)

# Create hexagonal grid
cat("Creating hexagonal grid...\n")
hex_grid <- st_make_grid(
  all_polygons_proj,
  cellsize = hex_size,
  square = FALSE,
  what = "polygons"
) %>%
  st_sf() %>%
  mutate(hex_id = row_number())

# Filter to relevant hexes only
cat("Filtering to relevant hexes...\n")
all_outages_union <- st_union(all_polygons_proj)
relevant_hex_indices <- st_intersects(hex_grid, all_outages_union, sparse = TRUE)
hex_grid_filtered <- hex_grid[lengths(relevant_hex_indices) > 0, ]

cat(sprintf("Reduced from %d to %d relevant hexes (%.1f%%)\n", 
            nrow(hex_grid), 
            nrow(hex_grid_filtered),
            100 * nrow(hex_grid_filtered) / nrow(hex_grid)))

# Initialize count column
hex_grid_filtered$outage_count <- 0

# Pre-transform all data to projected CRS
cat("Transforming outage data...\n")
outage_data_proj <- map(outage_data, ~st_transform(.x, 32618))

# Calculate intersections
cat("Calculating intersections...\n")
total_files <- length(outage_data_proj)

for (i in seq_along(outage_data_proj)) {
  if (i %% 50 == 0) {
    cat(sprintf("  Processing file %d/%d...\n", i, total_files))
  }
  
  current_union <- st_union(outage_data_proj[[i]])
  intersects_sparse <- st_intersects(hex_grid_filtered, current_union)
  intersecting_indices <- which(lengths(intersects_sparse) > 0)
  hex_grid_filtered$outage_count[intersecting_indices] <- 
    hex_grid_filtered$outage_count[intersecting_indices] + 1
}

# Calculate outage score as percentage
hex_grid_filtered$outage_score <- (hex_grid_filtered$outage_count / total_files) * 100

cat(sprintf("\nHexes with outages: %d\n", nrow(hex_grid_filtered)))
cat(sprintf("Score range: %.1f%% - %.1f%%\n", 
            min(hex_grid_filtered$outage_score), 
            max(hex_grid_filtered$outage_score)))

# Transform back to WGS84 for export
cat("Transforming back to WGS84...\n")
hex_grid_wgs84 <- st_transform(hex_grid_filtered, 4326)

# === EXPORT 1: GeoJSON ===
cat("Exporting GeoJSON...\n")
geojson_path <- file.path(output_dir, "outage_hex_grid.geojson")
st_write(hex_grid_wgs84, geojson_path, delete_dsn = TRUE, quiet = TRUE)
cat(sprintf("  ✓ GeoJSON saved: %s\n", geojson_path))

# === EXPORT 2: Shapefile ===
cat("Exporting Shapefile...\n")
shapefile_dir <- file.path(output_dir, "shapefile")
dir_create(shapefile_dir)
shapefile_path <- file.path(shapefile_dir, "outage_hex_grid.shp")
st_write(hex_grid_wgs84, shapefile_path, delete_dsn = TRUE, quiet = TRUE)
cat(sprintf("  ✓ Shapefile saved: %s\n", shapefile_path))

# === EXPORT 3: CSV with statistics ===
cat("Exporting CSV statistics...\n")
csv_data <- hex_grid_wgs84 %>%
  st_drop_geometry() %>%
  select(hex_id, outage_count, outage_score) %>%
  arrange(desc(outage_score))

csv_path <- file.path(output_dir, "outage_hex_stats.csv")
write.csv(csv_data, csv_path, row.names = FALSE)
cat(sprintf("  ✓ CSV saved: %s\n", csv_path))

# === Create summary statistics ===
cat("Creating summary statistics...\n")
summary_stats <- hex_grid_filtered %>%
  st_drop_geometry() %>%
  summarise(
    total_hexes = n(),
    mean_score = mean(outage_score),
    median_score = median(outage_score),
    min_score = min(outage_score),
    max_score = max(outage_score),
    total_files_analyzed = total_files
  )

print(summary_stats)

# Save summary as JSON
summary_json_path <- file.path(output_dir, "summary_stats.json")
jsonlite::write_json(summary_stats, summary_json_path, pretty = TRUE)
cat(sprintf("  ✓ Summary JSON saved: %s\n", summary_json_path))

# === Create a simple HTML viewer ===
cat("Creating HTML viewer...\n")
html_content <- sprintf('<!DOCTYPE html>
<html>
<head>
    <title>HQ Outage Exposure Map</title>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
    <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
    <style>
        body { margin: 0; padding: 0; font-family: Arial, sans-serif; }
        #map { position: absolute; top: 0; bottom: 0; width: 100%%; }
        .info { 
            padding: 6px 8px; 
            background: white; 
            background: rgba(255,255,255,0.9); 
            box-shadow: 0 0 15px rgba(0,0,0,0.2); 
            border-radius: 5px; 
        }
        .info h4 { margin: 0 0 5px; color: #777; }
        .legend { 
            line-height: 18px; 
            color: #555; 
        }
        .legend i { 
            width: 18px; 
            height: 18px; 
            float: left; 
            margin-right: 8px; 
            opacity: 0.7; 
        }
    </style>
</head>
<body>
    <div id="map"></div>
    <script>
        // Initialize map centered on Quebec
        var map = L.map("map").setView([46.8, -71.2], 8);
        
        // Add base map
        L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
            attribution: "© OpenStreetMap contributors"
        }).addTo(map);
        
        // Load and display the GeoJSON
        fetch("outage_hex_grid.geojson")
            .then(response => response.json())
            .then(data => {
                L.geoJSON(data, {
                    style: function(feature) {
                        var score = feature.properties.outage_score;
                        return {
                            fillColor: getColor(score),
                            weight: 0.5,
                            opacity: 1,
                            color: "white",
                            fillOpacity: 0.7
                        };
                    },
                    onEachFeature: function(feature, layer) {
                        layer.bindPopup(
                            "<b>Hex ID:</b> " + feature.properties.hex_id + "<br>" +
                            "<b>Outage Score:</b> " + feature.properties.outage_score.toFixed(1) + "%%<br>" +
                            "<b>Count:</b> " + feature.properties.outage_count
                        );
                    }
                }).addTo(map);
            });
        
        // Color scale function
        function getColor(d) {
            return d > 80 ? "#800026" :
                   d > 60 ? "#BD0026" :
                   d > 40 ? "#E31A1C" :
                   d > 20 ? "#FC4E2A" :
                   d > 10 ? "#FD8D3C" :
                   d > 5  ? "#FEB24C" :
                   d > 2  ? "#FED976" :
                            "#FFEDA0";
        }
        
        // Add legend
        var legend = L.control({position: "bottomright"});
        legend.onAdd = function(map) {
            var div = L.DomUtil.create("div", "info legend");
            var grades = [0, 2, 5, 10, 20, 40, 60, 80];
            div.innerHTML = "<h4>Outage Score (%%)</h4>";
            for (var i = 0; i < grades.length; i++) {
                div.innerHTML +=
                    "<i style=\\"background:" + getColor(grades[i] + 1) + "\\"></i> " +
                    grades[i] + (grades[i + 1] ? "&ndash;" + grades[i + 1] + "<br>" : "+");
            }
            return div;
        };
        legend.addTo(map);
        
        // Add info panel
        var info = L.control();
        info.onAdd = function(map) {
            this._div = L.DomUtil.create("div", "info");
            this._div.innerHTML = "<h4>HQ Cumulative Outage Exposure</h4>" +
                                  "<p>Based on %d snapshots</p>" +
                                  "<p>Hex size: %dm</p>";
            return this._div;
        };
        info.addTo(map);
    </script>
</body>
</html>', total_files, hex_size)

html_path <- file.path(output_dir, "index.html")
writeLines(html_content, html_path)
cat(sprintf("  ✓ HTML viewer saved: %s\n", html_path))

cat("\n=== Analysis Complete ===\n")
cat(sprintf("Total files analyzed: %d\n", total_files))
cat(sprintf("Total hexes with outages: %d\n", nrow(hex_grid_filtered)))
cat(sprintf("Mean outage score: %.2f%%\n", mean(hex_grid_filtered$outage_score)))
cat(sprintf("\nOutputs saved to: %s/\n", output_dir))
cat("  - outage_hex_grid.geojson\n")
cat("  - shapefile/outage_hex_grid.shp\n")
cat("  - outage_hex_stats.csv\n")
cat("  - summary_stats.json\n")
cat("  - index.html\n")
