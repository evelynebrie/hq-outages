# HQ Outages Cumulative Exposure Analysis

This repository automatically processes HQ outage data from Dropbox and publishes hexagonal grid analysis to a public GitHub Pages site.

## What It Does

1. **Downloads** all GeoJSON polygon files from your Dropbox app folder
2. **Processes** them into a hexagonal grid analysis showing cumulative outage exposure
3. **Exports** three formats:
   - **GeoJSON**: `outage_hex_grid.geojson` (for web mapping)
   - **Shapefile**: `shapefile/outage_hex_grid.shp` (for GIS software)
   - **CSV**: `outage_hex_stats.csv` (for spreadsheet analysis)
4. **Publishes** to your public repository's GitHub Pages

## File Structure

```
your-repo/
├── .github/
│   └── workflows/
│       └── publish-cumulative-exposure.yml  # GitHub Actions workflow
├── R/
│   └── build_cumulative_hex.R              # Main analysis script
├── data/
│   └── daily/                               # Downloaded files (temporary)
└── public/                                  # Generated outputs
    ├── outage_hex_grid.geojson             # Hex grid with scores
    ├── outage_hex_stats.csv                # Statistics table
    ├── shapefile/
    │   └── outage_hex_grid.shp             # Shapefile format
    ├── summary_stats.json                  # Summary statistics
    └── index.html                          # Interactive map viewer
```

## Setup Instructions

### 1. Create the R Script

Create `R/build_cumulative_hex.R` with the provided script.

### 2. Update the Workflow

Replace `.github/workflows/publish-cumulative-exposure.yml` with the updated version.

### 3. Verify Secrets

Ensure these secrets are set in your repository settings:
- `DROPBOX_REFRESH_TOKEN`
- `DROPBOX_APP_KEY`
- `DROPBOX_APP_SECRET`
- `PUBLIC_REPO_TOKEN`

### 4. Test the Workflow

1. Go to Actions tab in GitHub
2. Select "Publish cumulative exposure"
3. Click "Run workflow"
4. Monitor the logs for any errors

## Output Files

### outage_hex_grid.geojson
Complete hexagonal grid with properties:
- `hex_id`: Unique identifier
- `outage_count`: Number of snapshots with outage
- `outage_score`: Percentage (0-100) of time affected

### outage_hex_stats.csv
Tabular format with columns:
```csv
hex_id,outage_count,outage_score
1,342,48.86
2,156,22.29
...
```

### shapefile/
Standard ESRI Shapefile format for use in ArcGIS, QGIS, etc.

### index.html
Interactive Leaflet map with:
- Color-coded hexagons
- Click for details
- Legend showing score ranges

## Configuration

Edit `R/build_cumulative_hex.R` to customize:

```r
hex_size <- 1000  # Hex size in meters (default: 1km)
```

## Troubleshooting

### "No GeoJSON files found"
- Check that files match pattern: `polygons_*.geojson`
- Verify Dropbox path in workflow is correct
- Check Dropbox permissions

### "Process completed with exit code 2"
- Usually means the R script failed
- Check R package installation step
- Verify all dependencies are installed

### "Broken pipe" error
- Often occurs with large file processing
- The script handles this automatically
- Check that all output files were created

## Viewing Results

After successful deployment, your data will be available at:
- **Web Map**: `https://evelynebrie.github.io/hq-outages-public/`
- **GeoJSON**: `https://evelynebrie.github.io/hq-outages-public/outage_hex_grid.geojson`
- **CSV**: `https://evelynebrie.github.io/hq-outages-public/outage_hex_stats.csv`
- **Shapefile**: Download from the gh-pages branch

## Schedule

The workflow runs:
- **Daily** at 5:00 AM UTC (cron: `0 5 * * *`)
- **Manually** via workflow dispatch button

## Technical Details

- **Hex Grid**: 1km hexagons (adjustable)
- **CRS**: UTM Zone 18N (EPSG:32618) for processing
- **Output CRS**: WGS84 (EPSG:4326)
- **Score Calculation**: (outage_count / total_snapshots) × 100

## License

[Your License Here]
