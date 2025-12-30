# ============================================
# IMPROVED ERROR HANDLING FOR HQ_FUNCTIONS.R
# Add this to your hq_functions.R file
# ============================================

# Improved version fetching with fallback mechanisms
get_version_robust <- function(retries = 3) {
  for (attempt in 1:retries) {
    tryCatch({
      # Try to get version from bisversion.json
      ver_obj <- get_json(paste0(BASE, "bisversion.json"))
      ver <- extract_version(ver_obj)
      
      if (!is.na(ver) && nzchar(ver) && grepl("^\\d{14}$", ver)) {
        cat("✓ Got version:", ver, "\n")
        return(ver)
      }
      
      cat("Warning: Invalid version format, trying alternative...\n")
      
    }, error = function(e) {
      cat("Attempt", attempt, "failed:", conditionMessage(e), "\n")
    })
    
    # Fallback: try to get current timestamp-based version
    # HQ often uses format: YYYYMMDDHHmmss
    if (attempt == retries) {
      cat("Using fallback: generating version from current time\n")
      # Round to nearest 5-minute interval (HQ updates every ~5 min)
      now <- lubridate::with_tz(Sys.time(), "America/Toronto")
      rounded_min <- floor(lubridate::minute(now) / 5) * 5
      now <- lubridate::floor_date(now, "hour") + lubridate::minutes(rounded_min)
      ver <- format(now, "%Y%m%d%H%M%S")
      cat("Fallback version:", ver, "\n")
      return(ver)
    }
    
    Sys.sleep(2^attempt)
  }
  
  stop("Could not obtain valid version after ", retries, " attempts")
}

# REPLACE the get_hq_polygons function with this version:
get_hq_polygons <- function(add_metadata = TRUE, quiet = TRUE) {
  .hq_require_pkg("sf", "polygons output")
  
  # 1) Get version with robust retry logic
  ver <- get_version_robust(retries = 3)
  
  # 2) Download KMZ with retry
  kmz_url <- paste0(BASE, "bispoly", ver, ".kmz")
  tmp_kmz <- tempfile(fileext = ".kmz")
  tmp_dir <- tempfile()
  dir.create(tmp_dir, recursive = TRUE, showWarnings = FALSE)
  
  download_success <- FALSE
  for (attempt in 1:3) {
    r <- try(httr::GET(kmz_url, httr::write_disk(tmp_kmz, overwrite = TRUE), 
                       httr::timeout(60)), silent = TRUE)
    
    if (!inherits(r, "try-error") && httr::status_code(r) == 200) {
      download_success <- TRUE
      break
    }
    
    cat("Download attempt", attempt, "failed, retrying...\n")
    Sys.sleep(2)
  }
  
  if (!download_success) {
    stop("Failed to download polygons after 3 attempts from: ", kmz_url)
  }
  
  # 3) Try direct KMZ read; if it fails, unzip -> read KML
  poly <- try(sf::st_read(tmp_kmz, quiet = quiet), silent = TRUE)
  if (inherits(poly, "try-error")) {
    utils::unzip(tmp_kmz, exdir = tmp_dir)
    kml <- list.files(tmp_dir, pattern = "\\.kml$", full.names = TRUE)[1]
    if (is.na(kml)) stop("No .kml found inside the downloaded .kmz")
    poly <- sf::st_read(kml, quiet = quiet)
  }
  
  poly <- .hq_clean_polys(poly)
  
  if (isTRUE(add_metadata)) {
    TZ_local <- if (exists("TZ", inherits = FALSE)) get("TZ") else "America/Toronto"
    poly$hq_version      <- ver
    poly$hq_retrieved_at <- as.character(lubridate::with_tz(Sys.time(), TZ_local))
  }
  
  poly
}

# REPLACE the get_hq_outages function to use the robust version getter:
get_hq_outages <- function(return_sf = FALSE) {
  # 1) Get version with robust retry
  ver <- get_version_robust(retries = 3)
  
  # 2) Get outages payload with retry
  raw <- NULL
  for (attempt in 1:3) {
    raw <- try(get_json(paste0(BASE, "bismarkers", ver, ".json")), silent = TRUE)
    if (!inherits(raw, "try-error")) break
    cat("Outages fetch attempt", attempt, "failed, retrying...\n")
    Sys.sleep(2)
  }
  
  if (inherits(raw, "try-error")) {
    stop("Failed to fetch outages after 3 attempts")
  }
  
  # Rest of the function stays the same...
  pannes <- raw$pannes %||na% raw$markers %||na% raw$outages
  if (is.null(pannes)) {
    if (is.list(raw) && length(raw) > 0 && is.list(raw[[1]])) {
      pannes <- raw[[1]]
    }
  }
  
  if (is.null(pannes) || length(pannes) == 0) {
    message("No outages in payload.")
    return(tibble::tibble())
  }
  
  # [Rest of the existing function code...]
  df <- lapply(pannes, function(row) {
    row <- c(row, rep(NA, 16))[1:16]
    coords <- as.character(row[[5]])
    xy <- parse_coords(coords)
    
    tibble::tibble(
      customers        = suppressWarnings(as.integer(row[[1]])),
      started_at_raw   = as.character(row[[2]]),
      eta_raw          = as.character(row[[3]]),
      status_code      = as.character(row[[4]]),
      lon              = xy$lon,
      lat              = xy$lat,
      status2          = as.character(row[[6]]),
      unknown7         = as.character(row[[7]]),
      cause_code       = suppressWarnings(as.integer(row[[8]])),
      municipality_id  = suppressWarnings(as.integer(row[[9]])),
      message_id       = suppressWarnings(as.integer(row[[10]])),
      extra11          = row[[11]],
      extra12          = row[[12]]
    )
  }) |>
    dplyr::bind_rows()
  
  status_map <- c(
    "A" = "Assigned",
    "L" = "Crew working",
    "R" = "Crew en route",
    "P" = "Outage reported"
  )
  
  df <- df |>
    mutate(
      started_at = parse_dt(started_at_raw),
      eta_at     = parse_dt(eta_raw),
      status     = dplyr::recode(status_code, !!!status_map, .default = status_code),
      cause_group = dplyr::case_when(
        cause_code %in% 21:26 ~ "Weather",
        cause_code %in% 51:59 ~ "Vegetation / Animals",
        TRUE ~ NA_character_
      )
    ) |>
    select(-started_at_raw, -eta_raw)
  
  if (isTRUE(return_sf)) {
    if (!has_sf) stop("Package 'sf' not installed. Install it or call with return_sf = FALSE.")
    df <- df |> filter(!is.na(lon), !is.na(lat))
    df <- sf::st_as_sf(df, coords = c("lon", "lat"), crs = 4326)
  }
  
  df
}
