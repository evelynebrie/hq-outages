# Diagnostic script to check daily summary files
library(sf)

cat("=== DAILY SUMMARY DIAGNOSTIC ===\n\n")

daily_files <- list.files("public/daily", pattern = "^daily_.*\\.geojson$", full.names = TRUE)

if (length(daily_files) == 0) {
  cat("ERROR: No daily summary files found in public/daily/\n")
  quit()
}

cat(sprintf("Found %d daily summary files\n\n", length(daily_files)))

# Check first few files
for (i in 1:min(5, length(daily_files))) {
  f <- daily_files[i]
  date <- sub(".*daily_(.*)\\.geojson", "\\1", basename(f))
  
  data <- st_read(f, quiet = TRUE)
  
  cat(sprintf("[%d] Date: %s\n", i, date))
  cat(sprintf("    Hexes affected: %d\n", nrow(data)))
  cat(sprintf("    Count range: %d - %d (hours per hex)\n", min(data$count), max(data$count)))
  cat(sprintf("    Mean count: %.1f\n", mean(data$count)))
  
  # Check a sample hex
  sample_hex <- data[1,]
  dts <- strsplit(sample_hex$datetimes_affected, ",")[[1]]
  cat(sprintf("    Sample hex #%d:\n", sample_hex$hex_id))
  cat(sprintf("      - count: %d\n", sample_hex$count))
  cat(sprintf("      - datetimes: %d entries\n", length(dts)))
  cat(sprintf("      - First datetime: %s\n", dts[1]))
  if (length(dts) > 1) {
    cat(sprintf("      - Last datetime: %s\n", dts[length(dts)]))
  }
  
  # Extract hours from datetimes
  hours <- unique(sapply(strsplit(dts, " "), function(x) substr(x[2], 1, 2)))
  cat(sprintf("      - Unique hours: %d (%s)\n", length(hours), paste(hours, collapse=", ")))
  
  cat("\n")
}

cat("=== CHECKING DATA SOURCE FILES ===\n\n")

source_files <- list.files("data/daily", pattern = "^polygons_.*\\.geojson$")
cat(sprintf("Total source files: %d\n", length(source_files)))

# Group by date
dates_table <- table(substr(source_files, 10, 17))
cat("\nFiles per date:\n")
for (d in names(dates_table)) {
  formatted <- sprintf("%s-%s-%s", substr(d, 1, 4), substr(d, 5, 6), substr(d, 7, 8))
  cat(sprintf("  %s: %d files\n", formatted, dates_table[d]))
}

# Check hour distribution for first date
first_date_pattern <- names(dates_table)[1]
first_date_files <- source_files[grepl(first_date_pattern, source_files)]
cat(sprintf("\nHour distribution for %s:\n", names(dates_table)[1]))

hours_in_files <- substr(first_date_files, 19, 20)
hour_counts <- table(hours_in_files)
for (h in names(hour_counts)) {
  cat(sprintf("  Hour %s: %d file(s)\n", h, hour_counts[h]))
}

cat("\n=== DIAGNOSIS COMPLETE ===\n")
