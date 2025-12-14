# HTML-Only Generator for HQ Outages Map
# This script ONLY regenerates the HTML file from existing data
# Use this for quick UI fixes without reprocessing data

suppressPackageStartupMessages({
  library(jsonlite)
})

cat("=== Regenerating HTML Only ===\n")

output_dir <- "public"
hex_size_km <- 1  # 1km hexes

# Check if data exists
if (!file.exists(file.path(output_dir, "total", "total_exposure.geojson"))) {
  stop("ERROR: total_exposure.geojson not found. Run full processing first.")
}

# Count daily files for stats
daily_files <- list.files(file.path(output_dir, "daily"), pattern = "^daily_.*\\.geojson$")
num_days <- length(daily_files)

cat(sprintf("Found %d daily summary files\n", num_days))

# Load customer data if available
customer_data_js <- "null"
customer_json_path <- file.path(output_dir, "total", "customer_impact.json")
if (file.exists(customer_json_path)) {
  customer_data_js <- paste(readLines(customer_json_path), collapse = "\n")
  cat("Customer impact data found\n")
} else {
  cat("No customer impact data (optional)\n")
}

# Build HTML with all fixes
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
                      max-width:90vw;width:600px;max-height:85vh;overflow-y:auto}
        .detail-modal.active{display:block}
        .modal-overlay{display:none;position:fixed;z-index:1999;left:0;top:0;width:100%;height:100%;
                       background:rgba(0,0,0,0.5)}
        .modal-overlay.active{display:block}
        .close-modal{float:right;font-size:24px;font-weight:bold;cursor:pointer;color:#999;line-height:1}
        .close-modal:hover{color:#333}
        .detail-table{width:100%;border-collapse:collapse;margin-top:10px;font-size:13px}
        .detail-table th,.detail-table td{text-align:left;padding:8px;border-bottom:1px solid #eee}
        .detail-table th{font-weight:600;background:#f5f5f5;position:sticky;top:0}
        .detail-table tfoot td{font-weight:600;background:#f9f9f9;border-top:2px solid #ccc}
        .sidebar{position:fixed;left:0;top:0;bottom:0;width:350px;background:white;box-shadow:2px 0 10px rgba(0,0,0,0.1);
                 transform:translateX(0);transition:transform 0.3s;z-index:1000;overflow-y:auto}
        .sidebar.closed{transform:translateX(-350px)}
        .sidebar-toggle{position:fixed;left:10px;top:10px;z-index:1001;padding:10px 15px;background:white;
                        border:none;border-radius:4px;box-shadow:0 2px 10px rgba(0,0,0,0.1);cursor:pointer;font-size:14px}
        .sidebar-content{padding:20px}
        .sidebar h3{margin-top:0;font-size:16px}
        .chart-container{margin:20px 0;height:250px}
        @media (max-width: 768px) {
            .sidebar{width:100%;max-width:350px}
            .sidebar.closed{transform:translateX(-100%)}
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
    
    <button class="sidebar-toggle" onclick="toggleSidebar()" id="sidebarToggle">☰ Statistiques</button>
    <div class="sidebar" id="sidebar">
        <div class="sidebar-content">
            <h3>Impact sur les clients</h3>
            <p id="sidebarInfo">Chargement des données...</p>
            <div class="chart-container">
                <canvas id="impactChart"></canvas>
            </div>
        </div>
    </div>
    
    <script>
        var customerData = ', customer_data_js, ';
        var map = L.map("map").setView([46.2, -72.5], 9);
        L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png",{attribution:"© OSM"}).addTo(map);
        
        var currentLayer;
        var allData = {};
        var impactChart = null;
        var legendControl = null;
        
        function toggleSidebar() {
            const sidebar = document.getElementById("sidebar");
            const toggle = document.getElementById("sidebarToggle");
            sidebar.classList.toggle("closed");
            if (sidebar.classList.contains("closed")) {
                toggle.style.left = "10px";
            } else {
                toggle.style.left = "360px";
            }
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
                            const clean = dt.trim();
                            if (clean) {
                                const date = clean.split(' ')[0];
                                dates.add(date);
                            }
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
                } else {
                    document.getElementById('sidebarInfo').innerHTML = '<p>Aucune donnée client disponible</p>';
                }
                
            } catch (e) {
                console.error('Error loading data:', e);
                document.getElementById('sidebarInfo').innerHTML = '<p>Erreur de chargement</p>';
            }
        }
        
        function initImpactChart() {
            try {
                const ctx = document.getElementById('impactChart');
                if (!ctx) return;
                
                const dates = {};
                customerData.forEach(d => {
                    const date = d.datetime.split(' ')[0];
                    if (!dates[date]) dates[date] = 0;
                    dates[date] += d.total_customers_affected;
                });
                
                const sortedDates = Object.keys(dates).sort();
                const values = sortedDates.map(d => dates[d]);
                
                impactChart = new Chart(ctx, {
                    type: 'bar',
                    data: {
                        labels: sortedDates,
                        datasets: [{
                            label: 'Clients affectés',
                            data: values,
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
                                    text: 'Nombre'
                                }
                            }
                        }
                    }
                });
                
                const totalCustomers = customerData.reduce((sum, d) => sum + d.total_customers_affected, 0);
                const dateRange = sortedDates.length > 0 ? sortedDates[0] + ' à ' + sortedDates[sortedDates.length-1] : 'N/A';
                
                document.getElementById('sidebarInfo').innerHTML = 
                    '<p><strong>Total cumulé:</strong> ' + totalCustomers.toLocaleString() + ' clients</p>' +
                    '<p><strong>Période:</strong> ' + dateRange + '</p>';
            } catch (e) {
                console.error('Chart error:', e);
                document.getElementById('sidebarInfo').innerHTML = '<p>Erreur lors de la création du graphique</p>';
            }
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
                    const datetimes = f.properties.datetimes_affected.split(',').map(s => s.trim()).filter(s => s);
                    return datetimes.some(dt => {
                        if (hour === 'all') {
                            return dt.startsWith(date);
                        } else {
                            const targetDateTime = date + ' ' + hour.padStart(2, '0') + ':00';
                            return dt === targetDateTime;
                        }
                    });
                }).map(f => {
                    const datetimes = f.properties.datetimes_affected.split(',').map(s => s.trim()).filter(s => s);
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
                               '<b>Heures impactées:</b> ' + hoursCount + '<br>' +
                               '<b>Jours impactés:</b> ' + props.days_affected + '<br>' +
                               '<b>Centroïde:</b> ' + props.centroid_lat.toFixed(6) + ', ' + props.centroid_lon.toFixed(6) + '<br>' +
                               '<a class=\"detail-link\" onclick=\"showDetails(' + props.hex_id + ')\">Voir tous les détails</a>';
                    
                    layer.bindPopup(popup);
                }
            }).addTo(map);
            
            if (legendControl) {
                if (isFiltered) {
                    map.removeControl(legendControl);
                } else if (!map.hasControl(legendControl)) {
                    legendControl.addTo(map);
                }
            }
        }
        
        function showDetails(hexId) {
            const feature = allData.total.features.find(f => f.properties.hex_id === hexId);
            if (!feature) {
                alert('Données non trouvées pour cet hexagone');
                return;
            }
            
            const props = feature.properties;
            if (!props.datetimes_affected) {
                alert('Aucune donnée temporelle disponible');
                return;
            }
            
            const datetimes = props.datetimes_affected.split(',').map(s => s.trim()).filter(s => s).sort();
            
            if (datetimes.length === 0) {
                alert('Aucune donnée temporelle disponible');
                return;
            }
            
            const byDate = {};
            datetimes.forEach(dt => {
                const parts = dt.split(' ');
                if (parts.length !== 2) return;
                const date = parts[0];
                const time = parts[1];
                if (!byDate[date]) byDate[date] = [];
                const hourMatch = time.match(/^(\\d{1,2}):/);
                if (hourMatch) {
                    const hour = parseInt(hourMatch[1]);
                    byDate[date].push(hour);
                }
            });
            
            let html = '<h3>Hexagone #' + hexId + '</h3>';
            html += '<p><strong>Centroïde:</strong> ' + props.centroid_lat.toFixed(6) + ', ' + props.centroid_lon.toFixed(6) + '</p>';
            html += '<h4>Heures affectées par date:</h4>';
            html += '<table class=\"detail-table\">';
            html += '<thead><tr><th>Date</th><th>Heures</th></tr></thead><tbody>';
            
            const sortedDates = Object.keys(byDate).sort();
            sortedDates.forEach(date => {
                const hours = byDate[date].sort((a, b) => a - b);
                const hourList = hours.map(h => h + ':00').join(', ');
                html += '<tr><td>' + date + '</td><td>' + hourList + '</td></tr>';
            });
            
            const totalDays = sortedDates.length;
            const totalHours = Object.values(byDate).reduce((sum, arr) => sum + arr.length, 0);
            
            html += '</tbody><tfoot><tr><td><strong>Total</strong></td><td><strong>' + totalDays + ' jours, ' + totalHours + ' heures</strong></td></tr></tfoot>';
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
        
        legendControl = L.control({position:'bottomright'});
        legendControl.onAdd = () => {
            var div = L.DomUtil.create('div','info legend');
            div.innerHTML = '<h4>Heures</h4>';
            [0,2,5,10,20,40,60,80].forEach((g,i,a) => {
                div.innerHTML += '<i style=\"background:' + getColor(g+1) + '\"></i>' + g + (a[i+1]?'–'+a[i+1]:'+') + '<br>';
            });
            return div;
        };
        legendControl.addTo(map);
        
        var geocoder = L.Control.geocoder({
            defaultMarkGeocode: false,
            placeholder: 'Adresse ou code postal...',
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
            div.innerHTML = '<h4>Pannes cumulatives</h4>' +
                           '<p><b>Hexagones:</b> ", hex_size_km, " km</p>' +
                           '<p><b>Jours:</b> ", num_days, "</p>';
            return div;
        };
        info.addTo(map);
        
        var controls = L.control({position:'topleft'});
        controls.onAdd = () => {
            var div = L.DomUtil.create('div','controls');
            div.innerHTML = '<h4>Filtres</h4>' +
                           '<label>Date:</label>' +
                           '<select id=\"dateSelect\" onchange=\"updateMap()\"><option value=\"all\">Toutes (défaut)</option></select>' +
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

# Write HTML
html_path <- file.path(output_dir, "index.html")
writeLines(html, html_path)

cat(sprintf("\n✅ HTML regenerated: %s\n", html_path))
cat("All UI fixes applied:\n")
cat("  ✓ Fixed hour counting logic\n")
cat("  ✓ Better modal error handling\n")
cat("  ✓ List all hours in detail table\n")
cat("  ✓ Mobile-friendly modal (no welcome popup)\n")
cat("  ✓ Sidebar open by default with error handling\n")
cat("  ✓ Legend hides when filtering\n")
cat("  ✓ Closer default zoom (zoom 9)\n")

cat("\n🚀 Ready to deploy!\n")
