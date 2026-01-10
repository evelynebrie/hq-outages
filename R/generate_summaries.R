# Generate Summary Statistics and Reports
# Creates detailed analysis of hex outage patterns over time

suppressPackageStartupMessages({
  library(sf)
  library(dplyr)
  library(jsonlite)
  library(data.table)
})

cat("=== Summary Statistics Generator ===\n\n")

# Paths
output_dir <- "public"
summary_dir <- file.path(output_dir, "summaries")
dir.create(summary_dir, recursive = TRUE, showWarnings = FALSE)

# ==================================================================
# Load all data
# ==================================================================
cat("[1] Loading data...\n")

# Load total cumulative
total_file <- file.path(output_dir, "total", "total_exposure.geojson")
if (!file.exists(total_file)) {
  stop("Total exposure file not found. Run build_cumulative_hex_15min.R first.")
}

total_data <- st_read(total_file, quiet = TRUE)
cat(sprintf("  ✓ Loaded total data: %d hexagons\n", nrow(total_data)))

# Load daily summaries
daily_files <- list.files(file.path(output_dir, "daily"), 
                         pattern = "^daily_.*\\.geojson$", 
                         full.names = TRUE)
cat(sprintf("  ✓ Found %d daily summaries\n", length(daily_files)))

# Load monthly summaries
monthly_files <- list.files(file.path(output_dir, "monthly"), 
                           pattern = "^monthly_.*\\.geojson$", 
                           full.names = TRUE)
cat(sprintf("  ✓ Found %d monthly summaries\n", length(monthly_files)))

# ==================================================================
# Generate Overall Statistics
# ==================================================================
cat("\n[2] Generating overall statistics...\n")

overall_stats <- list(
  generated_at = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
  total_hexagons_affected = nrow(total_data),
  total_occurrences = sum(total_data$total_occurrences),
  max_occurrences_per_hex = max(total_data$total_occurrences),
  mean_occurrences_per_hex = mean(total_data$total_occurrences),
  median_occurrences_per_hex = median(total_data$total_occurrences),
  date_range = list(
    first = min(total_data$first_occurrence),
    last = max(total_data$last_occurrence)
  ),
  num_daily_summaries = length(daily_files),
  num_monthly_summaries = length(monthly_files)
)

# Distribution of occurrences
occurrence_breaks <- c(0, 1, 2, 5, 10, 20, 40, 60, 80, Inf)
occurrence_labels <- c("1", "2", "3-5", "6-10", "11-20", "21-40", "41-60", "61-80", "81+")

occurrence_dist <- total_data %>%
  st_drop_geometry() %>%
  mutate(occurrence_bin = cut(total_occurrences, 
                              breaks = occurrence_breaks,
                              labels = occurrence_labels,
                              include.lowest = TRUE)) %>%
  count(occurrence_bin, name = "count") %>%
  mutate(percentage = round(100 * count / sum(count), 2))

overall_stats$occurrence_distribution <- as.list(occurrence_dist)

# Top 20 most affected hexagons
top_hexes <- total_data %>%
  st_drop_geometry() %>%
  arrange(desc(total_occurrences)) %>%
  head(20) %>%
  select(hex_id, total_occurrences, days_affected, 
         centroid_lat, centroid_lon, 
         first_occurrence, last_occurrence)

overall_stats$top_20_hexagons <- as.list(top_hexes)

# Save overall statistics
write_json(overall_stats, 
          file.path(summary_dir, "overall_statistics.json"),
          pretty = TRUE,
          auto_unbox = TRUE)

cat("  ✓ Overall statistics saved\n")

# ==================================================================
# Generate Daily Statistics
# ==================================================================
cat("\n[3] Generating daily statistics...\n")

daily_stats_list <- list()

for (daily_file in daily_files) {
  date <- sub("daily_(.*)\\.geojson", "\\1", basename(daily_file))
  
  tryCatch({
    daily_data <- st_read(daily_file, quiet = TRUE)
    
    daily_stats_list[[date]] <- list(
      date = date,
      hexagons_affected = nrow(daily_data),
      total_occurrences_today = sum(daily_data$occurrences_today, na.rm = TRUE),
      max_occurrences = max(daily_data$occurrences_today, na.rm = TRUE),
      mean_occurrences = round(mean(daily_data$occurrences_today, na.rm = TRUE), 2),
      new_hexagons = sum(daily_data$cumulative_total == daily_data$occurrences_today, na.rm = TRUE)
    )
  }, error = function(e) {
    cat(sprintf("    ⚠ Error processing %s: %s\n", date, e$message))
  })
}

# Sort by date
daily_stats_list <- daily_stats_list[order(names(daily_stats_list))]

# Save daily statistics
write_json(list(daily_summaries = daily_stats_list),
          file.path(summary_dir, "daily_statistics.json"),
          pretty = TRUE,
          auto_unbox = TRUE)

cat(sprintf("  ✓ Daily statistics saved for %d dates\n", length(daily_stats_list)))

# ==================================================================
# Generate Monthly Statistics
# ==================================================================
cat("\n[4] Generating monthly statistics...\n")

monthly_stats_list <- list()

for (monthly_file in monthly_files) {
  month <- sub("monthly_(.*)\\.geojson", "\\1", basename(monthly_file))
  
  tryCatch({
    monthly_data <- st_read(monthly_file, quiet = TRUE)
    
    monthly_stats_list[[month]] <- list(
      month = month,
      hexagons_affected = nrow(monthly_data),
      total_occurrences_month = sum(monthly_data$occurrences_month, na.rm = TRUE),
      total_days_affected = sum(monthly_data$days_affected, na.rm = TRUE),
      max_occurrences = max(monthly_data$occurrences_month, na.rm = TRUE),
      mean_occurrences = round(mean(monthly_data$occurrences_month, na.rm = TRUE), 2),
      mean_days_affected = round(mean(monthly_data$days_affected, na.rm = TRUE), 2)
    )
  }, error = function(e) {
    cat(sprintf("    ⚠ Error processing %s: %s\n", month, e$message))
  })
}

# Sort by month
monthly_stats_list <- monthly_stats_list[order(names(monthly_stats_list))]

# Save monthly statistics
write_json(list(monthly_summaries = monthly_stats_list),
          file.path(summary_dir, "monthly_statistics.json"),
          pretty = TRUE,
          auto_unbox = TRUE)

cat(sprintf("  ✓ Monthly statistics saved for %d months\n", length(monthly_stats_list)))

# ==================================================================
# Generate Time Series Data
# ==================================================================
cat("\n[5] Generating time series data...\n")

# Create time series of daily hex counts
daily_time_series <- data.frame(
  date = names(daily_stats_list),
  hexagons_affected = sapply(daily_stats_list, function(x) x$hexagons_affected),
  total_occurrences = sapply(daily_stats_list, function(x) x$total_occurrences_today),
  stringsAsFactors = FALSE
)

write.csv(daily_time_series,
         file.path(summary_dir, "daily_time_series.csv"),
         row.names = FALSE)

cat("  ✓ Daily time series saved\n")

# Create time series of monthly hex counts
monthly_time_series <- data.frame(
  month = names(monthly_stats_list),
  hexagons_affected = sapply(monthly_stats_list, function(x) x$hexagons_affected),
  total_occurrences = sapply(monthly_stats_list, function(x) x$total_occurrences_month),
  stringsAsFactors = FALSE
)

write.csv(monthly_time_series,
         file.path(summary_dir, "monthly_time_series.csv"),
         row.names = FALSE)

cat("  ✓ Monthly time series saved\n")

# ==================================================================
# Generate Human-Readable Summary Report
# ==================================================================
cat("\n[6] Generating summary report...\n")

report_lines <- c(
  "=================================================================",
  "HQ OUTAGES - CUMULATIVE ANALYSIS REPORT",
  "=================================================================",
  "",
  sprintf("Generated: %s", overall_stats$generated_at),
  "",
  "OVERALL STATISTICS",
  "-----------------------------------------------------------------",
  sprintf("Total hexagons affected: %s", 
          format(overall_stats$total_hexagons_affected, big.mark = ",")),
  sprintf("Total occurrence count: %s", 
          format(overall_stats$total_occurrences, big.mark = ",")),
  sprintf("Maximum occurrences (single hex): %d", 
          overall_stats$max_occurrences_per_hex),
  sprintf("Mean occurrences per hex: %.1f", 
          overall_stats$mean_occurrences_per_hex),
  sprintf("Median occurrences per hex: %.0f", 
          overall_stats$median_occurrences_per_hex),
  "",
  sprintf("Date range: %s to %s", 
          overall_stats$date_range$first,
          overall_stats$date_range$last),
  "",
  "OCCURRENCE DISTRIBUTION",
  "-----------------------------------------------------------------"
)

for (i in seq_along(occurrence_dist$occurrence_bin)) {
  report_lines <- c(report_lines,
                   sprintf("  %8s occurrences: %6d hexagons (%5.1f%%)",
                          occurrence_dist$occurrence_bin[i],
                          occurrence_dist$count[i],
                          occurrence_dist$percentage[i]))
}

report_lines <- c(report_lines,
  "",
  "TOP 10 MOST AFFECTED HEXAGONS",
  "-----------------------------------------------------------------",
  sprintf("%-8s  %12s  %12s  %20s", 
          "Hex ID", "Occurrences", "Days", "Location")
)

for (i in 1:min(10, nrow(top_hexes))) {
  report_lines <- c(report_lines,
                   sprintf("%-8d  %12d  %12d  %9.5f, %10.5f",
                          top_hexes$hex_id[i],
                          top_hexes$total_occurrences[i],
                          top_hexes$days_affected[i],
                          top_hexes$centroid_lat[i],
                          top_hexes$centroid_lon[i]))
}

if (length(daily_stats_list) > 0) {
  report_lines <- c(report_lines,
    "",
    "RECENT DAILY ACTIVITY (Last 7 Days)",
    "-----------------------------------------------------------------"
  )
  
  recent_days <- tail(names(daily_stats_list), 7)
  for (day in recent_days) {
    stats <- daily_stats_list[[day]]
    report_lines <- c(report_lines,
                     sprintf("%s: %d hexagons affected, %d occurrences",
                            stats$date,
                            stats$hexagons_affected,
                            stats$total_occurrences_today))
  }
}

if (length(monthly_stats_list) > 0) {
  report_lines <- c(report_lines,
    "",
    "MONTHLY SUMMARY",
    "-----------------------------------------------------------------"
  )
  
  for (month in names(monthly_stats_list)) {
    stats <- monthly_stats_list[[month]]
    report_lines <- c(report_lines,
                     sprintf("%s: %d hexagons, %d occurrences, %.1f avg days affected",
                            stats$month,
                            stats$hexagons_affected,
                            stats$total_occurrences_month,
                            stats$mean_days_affected))
  }
}

report_lines <- c(report_lines,
  "",
  "=================================================================",
  "End of Report",
  "================================================================="
)

writeLines(report_lines, file.path(summary_dir, "summary_report.txt"))

# Also print to console
cat("\n")
cat(paste(report_lines, collapse = "\n"))
cat("\n")

cat("\n✅ Summary generation complete!\n")
cat(sprintf("   Output directory: %s\n", summary_dir))
cat("   Files created:\n")
cat("     • overall_statistics.json\n")
cat("     • daily_statistics.json\n")
cat("     • monthly_statistics.json\n")
cat("     • daily_time_series.csv\n")
cat("     • monthly_time_series.csv\n")
cat("     • summary_report.txt\n")
