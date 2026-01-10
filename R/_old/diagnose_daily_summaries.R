name: Diagnostic - Download and Analyze Source Data

on:
  workflow_dispatch:

jobs:
  diagnose:
    runs-on: ubuntu-latest
    
    steps:
      - uses: actions/checkout@v3
      
      - name: Setup R
        uses: r-lib/actions/setup-r@v2
        with:
          use-public-rspm: true

      - name: System libraries
        run: |
          sudo apt-get update
          sudo apt-get install -y libgdal-dev libgeos-dev libproj-dev libudunits2-dev jq curl

      - name: Install R packages
        run: |
          Rscript -e 'install.packages(c("sf", "data.table"), repos="https://cloud.r-project.org")'

      # Get fresh Dropbox token
      - name: Get Fresh Dropbox Access Token
        id: get_token
        env:
          DROPBOX_REFRESH_TOKEN: ${{ secrets.DROPBOX_REFRESH_TOKEN }}
          DROPBOX_APP_KEY: ${{ secrets.DROPBOX_APP_KEY }}
          DROPBOX_APP_SECRET: ${{ secrets.DROPBOX_APP_SECRET }}
        run: |
          echo "🔑 Getting fresh access token..."
          
          RESPONSE=$(curl -s -X POST https://api.dropbox.com/oauth2/token \
            -u "${DROPBOX_APP_KEY}:${DROPBOX_APP_SECRET}" \
            -d "grant_type=refresh_token&refresh_token=${DROPBOX_REFRESH_TOKEN}")
          
          ACCESS_TOKEN=$(echo "$RESPONSE" | jq -r '.access_token // empty')
          
          if [ -z "$ACCESS_TOKEN" ]; then
            echo "❌ Failed to get access token"
            echo "$RESPONSE" | jq '.'
            exit 1
          fi
          
          echo "✅ Token obtained"
          echo "access_token=$ACCESS_TOKEN" >> $GITHUB_OUTPUT

      # Download files with pagination
      - name: Download files from Dropbox
        env:
          DROPBOX_TOKEN: ${{ steps.get_token.outputs.access_token }}
        run: |
          echo "📥 Listing files in Dropbox..."
          mkdir -p data/daily
          
          # Initialize
          ALL_FILES=""
          HAS_MORE=true
          CURSOR=""
          PAGE=1
          
          # Paginate through all files (similar to publish-cumulative)
          while [ "$HAS_MORE" = "true" ]; do
            echo "  Fetching page $PAGE..."
            
            if [ -z "$CURSOR" ]; then
              RESPONSE=$(curl -s -X POST https://api.dropboxapi.com/2/files/list_folder \
                -H "Authorization: Bearer $DROPBOX_TOKEN" \
                -H "Content-Type: application/json" \
                -d '{"path": "/hq-outages", "recursive": true}')
            else
              RESPONSE=$(curl -s -X POST https://api.dropboxapi.com/2/files/list_folder/continue \
                -H "Authorization: Bearer $DROPBOX_TOKEN" \
                -H "Content-Type: application/json" \
                -d "{\"cursor\": \"$CURSOR\"}")
            fi
            
            # Check for errors
            if echo "$RESPONSE" | jq -e '.error' > /dev/null 2>&1; then
              echo "❌ Error from Dropbox API:"
              echo "$RESPONSE" | jq '.error'
              exit 1
            fi
            
            # Check if response has entries
            if ! echo "$RESPONSE" | jq -e '.entries' > /dev/null 2>&1; then
              echo "⚠️ Unexpected response (no .entries field)"
              echo "$RESPONSE"
              break
            fi
            
            PAGE_COUNT=$(echo "$RESPONSE" | jq '.entries | length')
            echo "    Found $PAGE_COUNT entries"
            
            # Extract file paths
            PAGE_FILES=$(echo "$RESPONSE" | jq -r '.entries[]? | select(.".tag" == "file") | .path_display' || true)
            
            if [ ! -z "$PAGE_FILES" ]; then
              if [ -z "$ALL_FILES" ]; then
                ALL_FILES="$PAGE_FILES"
              else
                ALL_FILES="$ALL_FILES"$'\n'"$PAGE_FILES"
              fi
            fi
            
            # Check if there are more pages
            HAS_MORE=$(echo "$RESPONSE" | jq -r '.has_more')
            
            if [ "$HAS_MORE" = "true" ]; then
              CURSOR=$(echo "$RESPONSE" | jq -r '.cursor')
              PAGE=$((PAGE + 1))
            fi
            
            # Safety: max 10 pages
            if [ $PAGE -gt 10 ]; then
              echo "⚠️ Stopping after 10 pages"
              break
            fi
          done
          
          echo "✅ Completed pagination: $PAGE page(s)"
          
          # Count total files
          TOTAL_COUNT=0
          if [ ! -z "$ALL_FILES" ]; then
            TOTAL_COUNT=$(echo "$ALL_FILES" | wc -l)
          fi
          echo "📊 Total files found: $TOTAL_COUNT"
          
          if [ $TOTAL_COUNT -eq 0 ]; then
            echo "❌ No files found in Dropbox!"
            exit 1
          fi
          
          # Filter for polygon files
          POLYGONS=$(echo "$ALL_FILES" | grep "/polygons_.*\.geojson$" || true)
          POLY_COUNT=$(echo "$POLYGONS" | wc -l 2>/dev/null || echo "0")
          
          echo "📍 Polygon files: $POLY_COUNT"
          
          if [ "$POLY_COUNT" -eq 0 ]; then
            echo "❌ No polygon files found!"
            exit 1
          fi
          
          # Analyze file distribution BEFORE downloading
          echo ""
          echo "========================================="
          echo "FILE DISTRIBUTION IN DROPBOX"
          echo "========================================="
          echo ""
          
          # Extract dates from filenames
          echo "$POLYGONS" | while read file; do
            fname=$(basename "$file")
            # Extract YYYYMMDD from filename
            echo "$fname" | grep -oP '\d{8}' | head -1
          done | sort | uniq -c | sort -rn | head -20 > /tmp/date_dist.txt
          
          cat /tmp/date_dist.txt
          
          echo ""
          UNIQUE_DATES=$(cat /tmp/date_dist.txt | wc -l)
          echo "Total unique dates: $UNIQUE_DATES"
          
          # Check if dates have multiple files (hourly scraping)
          FIRST_LINE=$(head -1 /tmp/date_dist.txt)
          FILES_PER_DATE=$(echo "$FIRST_LINE" | awk '{print $1}')
          echo "Files for most common date: $FILES_PER_DATE"
          
          if [ "$FILES_PER_DATE" -eq 1 ]; then
            echo ""
            echo "⚠️ WARNING: Only 1 file per date!"
            echo "   This means you're not scraping hourly"
            echo "   Each hex will show count=1"
          elif [ "$FILES_PER_DATE" -lt 10 ]; then
            echo ""
            echo "⚠️ WARNING: Only $FILES_PER_DATE files per date"
            echo "   Expected ~24 for hourly scraping"
          else
            echo ""
            echo "✅ Good: $FILES_PER_DATE files per date (hourly scraping working)"
          fi
          
          # Download sample files (recent dates)
          echo ""
          echo "========================================="
          echo "DOWNLOADING SAMPLE FILES"
          echo "========================================="
          echo ""
          
          # Get files from last 3 dates
          RECENT_DATES=$(cat /tmp/date_dist.txt | head -3 | awk '{print $2}')
          
          DOWNLOADED=0
          FAILED=0
          
          for date in $RECENT_DATES; do
            echo "Date: $date"
            
            # Get files for this date
            DATE_FILES=$(echo "$POLYGONS" | grep "$date" | head -30)
            
            for file in $DATE_FILES; do
              fname=$(basename "$file")
              
              if curl -s -f -X POST https://content.dropboxapi.com/2/files/download \
                -H "Authorization: Bearer $DROPBOX_TOKEN" \
                -H "Dropbox-API-Arg: {\"path\": \"$file\"}" \
                -o "data/daily/$fname" 2>/dev/null; then
                
                if [ -s "data/daily/$fname" ]; then
                  ((DOWNLOADED++))
                  [ $DOWNLOADED -le 5 ] && echo "  ✓ $fname"
                else
                  rm -f "data/daily/$fname"
                  ((FAILED++))
                fi
              else
                ((FAILED++))
              fi
              
              # Limit total downloads
              if [ $DOWNLOADED -ge 50 ]; then
                break 2
              fi
            done
          done
          
          echo ""
          echo "Downloaded: $DOWNLOADED files"
          echo "Failed: $FAILED files"
          
          if [ $DOWNLOADED -eq 0 ]; then
            echo "❌ Failed to download any files!"
            exit 1
          fi

      - name: Analyze downloaded files
        run: |
          echo ""
          echo "========================================="
          echo "ANALYZING DOWNLOADED FILES"
          echo "========================================="
          echo ""
          
          cat > analyze.R << 'EOF'
suppressPackageStartupMessages(library(sf))

files <- list.files("data/daily", pattern = "^polygons_.*\\.geojson$", full.names = TRUE)

if (length(files) == 0) {
  cat("❌ No files to analyze!\n")
  quit(status = 1)
}

cat(sprintf("Analyzing %d files\n\n", length(files)))

# Parse timestamps
basenames <- basename(files)
timestamps <- regmatches(basenames, regexpr("\\d{8}t\\d{6}", basenames, ignore.case = TRUE))

dates <- sprintf("%s-%s-%s", 
                substr(timestamps, 1, 4), 
                substr(timestamps, 5, 6), 
                substr(timestamps, 7, 8))

hours <- as.integer(substr(timestamps, 10, 11))

# Group by date
files_per_date <- table(dates)

cat("FILES PER DATE:\n")
for (d in names(files_per_date)) {
  cat(sprintf("  %s: %d files\n", d, files_per_date[d]))
}

cat("\nHOUR DISTRIBUTION:\n")
hour_dist <- table(hours)
print(hour_dist)

# Analyze content of a few files
cat("\n========================================\n")
cat("FILE CONTENT ANALYSIS\n")
cat("========================================\n\n")

for (i in 1:min(3, length(files))) {
  f <- files[i]
  cat(sprintf("[%d] %s\n", i, basename(f)))
  
  tryCatch({
    data <- st_read(f, quiet = TRUE)
    cat(sprintf("    Polygons: %d\n", nrow(data)))
    cat(sprintf("    CRS: %s\n", st_crs(data)$input))
    
    # Check columns
    cat("    Columns:", paste(names(data), collapse=", "), "\n")
    
    cat("\n")
  }, error = function(e) {
    cat(sprintf("    ❌ Error: %s\n\n", e$message))
  })
}

cat("========================================\n")
cat("DIAGNOSIS\n")
cat("========================================\n\n")

max_files_per_date <- max(files_per_date)
min_files_per_date <- min(files_per_date)

cat(sprintf("Files per date: min=%d, max=%d\n", min_files_per_date, max_files_per_date))

if (max_files_per_date == 1) {
  cat("\n❌ PROBLEM FOUND: Only 1 file per date!\n")
  cat("\nThis means:\n")
  cat("  - Your hq-hourly.yml is NOT running every hour\n")
  cat("  - OR it's running but not saving unique files\n")
  cat("  - Result: Daily summaries show count=1 for everything\n")
  cat("\nFix:\n")
  cat("  - Check if hq-hourly.yml cron schedule is correct\n")
  cat("  - Verify workflow is actually running hourly\n")
  cat("  - Check Dropbox folder structure\n")
} else if (max_files_per_date < 20) {
  cat(sprintf("\n⚠️ WARNING: Only %d files per date\n", max_files_per_date))
  cat("  Expected ~24 for full hourly coverage\n")
} else {
  cat("\n✅ Good: Multiple files per date\n")
  cat("  Hourly scraping appears to be working\n")
}
EOF
          
          Rscript analyze.R

      - name: Summary
        run: |
          echo ""
          echo "========================================="
          echo "DIAGNOSTIC COMPLETE"
          echo "========================================="
          echo ""
          echo "Review the output above to see:"
          echo ""
          echo "1️⃣  File distribution in Dropbox"
          echo "     How many files per date?"
          echo ""
          echo "2️⃣  Whether hourly scraping is working"
          echo "     Should have ~24 files/date"
          echo ""
          echo "3️⃣  What the actual data looks like"
          echo "     Number of polygons per file"
          echo ""
          echo "If you see 'Only 1 file per date':"
          echo "  → Fix your hq-hourly.yml schedule"
          echo "  → Make sure it runs every hour"
          echo ""
          echo "If you see '24 files per date':"
          echo "  → Hourly scraping works!"
          echo "  → Problem is in aggregation logic"
          echo "  → Use build_cumulative_hex_v9_FULLY_FIXED.R"
          echo ""
          echo "========================================="
