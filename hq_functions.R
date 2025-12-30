# ============================================
# HQ_FUNCTIONS.R - UPDATED FOR ARCGIS API
# ============================================

library(httr)
library(jsonlite)
library(sf)
library(dplyr)
library(lubridate)

# NEW: ArcGIS FeatureServer endpoint
ARCGIS_LAYER3_URL <- "https://services5.arcgis.com/0akaykIdiPuMhFIy/arcgis/rest/services/bs_infoPannes_prd_vue/FeatureServer/3/query"

# OLD API base (keeping for backward compatibility if needed)
BASE <- "https://pannes.hydroquebec.com/pannes/donnees/v3_0/"

# Helper: require package
.hq_require_pkg <- function(pkg, purpose = "this operation") {
  if (!requireNamespace(pkg, quietly = TRUE)) {
    stop("Package '", pkg, "' is required for ", purpose, 
         ". Install it with: install.packages('", pkg, "')")
  }
}

# Helper: null coalescing
`%||na%` <- function(x, y) if (is.null(x)) y else x

# Helper: check if sf is available
has_sf <- requireNamespace("sf", quietly = TRUE)

# Helper: parse coordinates
parse_coords <- function(coords_str) {
  parts <- strsplit(as.character(coords_str), ",")[[1]]
  if (length(parts) != 2) return(list(lon = NA_real_, lat = NA_real_))
  list(lon = suppressWarnings(as.numeric(parts[1])),
       lat = suppressWarnings(as.numeric(parts[2])))
}

# Helper: parse datetime
parse_dt <- function(x, tz = "America/Toronto") {
  suppressWarnings({
    out <- lubridate::mdy_hm(x, tz = tz)
    out[is.na(out)] <- lubridate::ymd_hms(x[is.na(out)], tz = tz)
    out
  })
}

# Helper: get JSON
get_json <- function(url, ...) {
  r <- httr::GET(url, ...)
  httr::stop_for_status(r)
  jsonlite::fromJSON(httr::content(r, as = "text", encoding = "UTF-8"), 
                     simplifyVector = FALSE)
}

# Helper: extract version from old API
extract_version <- function(obj) {
  if (is.list(obj) && "version" %in% names(obj)) {
    return(as.character(obj$version))
  }
  NA_character_
}

# ============================================
# NEW: Get polygons from ArcGIS FeatureServer
# ============================================
get_hq_polygons <- function(add_metadata = TRUE, quiet = TRUE, retries = 3) {
  .hq_require_pkg("sf", "polygons output")
  
  cat("Fetching polygons from ArcGIS FeatureServer...\n")
  
  # Query parameters for ArcGIS
  params <- list(
    where = "1=1",
    outFields = "*",
    returnGeometry = "true",
    f = "geojson"
  )
  
  # Retry logic
  geojson_data <- NULL
  for (attempt in 1:retries) {
    tryCatch({
      response <- httr::GET(ARCGIS_LAYER3_URL, query = params, httr::timeout(60))
      
      if (httr::status_code(response) == 200) {
        geojson_text <- httr::content(response, as = "text", encoding = "UTF-8")
        geojson_data <- jsonlite::fromJSON(geojson_text, simplifyVector = FALSE)
        cat("✓ Successfully fetched polygon data\n")
        break
      } else {
        cat(sprintf("✗ HTTP %d on attempt %d/%d\n", 
                    httr::status_code(response), attempt, retries))
      }
    }, error = function(e) {
      cat(sprintf("✗ Error on attempt %d/%d: %s\n", attempt, retries, e$message))
    })
    
    if (attempt < retries) Sys.sleep(2)
  }
  
  if (is.null(geojson_data)) {
    stop("Failed to fetch polygons after ", retries, " attempts")
  }
  
  # Convert to sf object
  polygons_sf <- sf::st_read(jsonlite::toJSON(geojson_data, auto_unbox = TRUE), 
                              quiet = quiet)
  
  # Transform to match expected format
  TZ_local <- if (exists("TZ", inherits = FALSE)) get("TZ") else "America/Toronto"
  
  polygons_sf <- polygons_sf %>%
    dplyr::mutate(
      # Convert dateCreation from milliseconds to datetime
      timestamp = lubridate::as_datetime(dateCreation / 1000, tz = TZ_local),
      
      # Create formatted fields
      Name = idInterruption,
      description = "",
      begin = format(timestamp, "%Y-%m-%dT%H:%M:%S"),
      end = "",
      altitudeMode = "clampToGround",
      tessellate = -1L,
      extrude = 0L,
      visibility = -1L,
      drawOrder = 0L,
      icon = ""
    )
  
  # Calculate centroid
  centroids <- sf::st_centroid(polygons_sf$geometry)
  coords <- sf::st_coordinates(centroids)
  polygons_sf$centroid <- paste0(
    round(coords[, 1], 6), ",", round(coords[, 2], 6)
  )
  
  # Add metadata if requested
  if (isTRUE(add_metadata)) {
    polygons_sf$hq_version <- format(polygons_sf$timestamp, "%Y%m%d%H%M%S")
    polygons_sf$hq_retrieved_at <- format(
      lubridate::with_tz(Sys.time(), TZ_local), 
      "%Y-%m-%dT%H:%M:%S"
    )
  }
  
  # Select final columns
  polygons_sf <- polygons_sf %>%
    dplyr::select(
      Name, description, timestamp, begin, end, altitudeMode,
      tessellate, extrude, visibility, drawOrder, icon,
      centroid, hq_version, hq_retrieved_at, geometry
    )
  
  cat(sprintf("✓ Transformed %d polygons\n", nrow(polygons_sf)))
  
  polygons_sf
}

# ============================================
# KEEP OLD FUNCTIONS FOR OUTAGES DATA
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
    if (attempt == retries) {
      cat("Using fallback: generating version from current time\n")
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

# Get outages (keeping old API since polygons come from new API)
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
  
  # Parse outages
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

# Clean polygons (helper function)
.hq_clean_polys <- function(poly) {
  poly
}

# Join outages with polygons
hq_outages_join_polygons <- function(polygons_sf, outages_sf) {
  if (!has_sf) {
    stop("Package 'sf' required for spatial join")
  }
  
  # Ensure same CRS
  if (sf::st_crs(polygons_sf) != sf::st_crs(outages_sf)) {
    outages_sf <- sf::st_transform(outages_sf, sf::st_crs(polygons_sf))
  }
  
  # Spatial join
  joined <- sf::st_join(outages_sf, polygons_sf, join = sf::st_within)
  
  # Summarize by polygon
  summary <- joined %>%
    sf::st_drop_geometry() %>%
    dplyr::group_by(Name) %>%
    dplyr::summarise(
      n_outages = dplyr::n(),
      customers_sum = sum(customers, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    dplyr::rename(poly_id = Name)
  
  list(
    joined = joined,
    summary = summary
  )
}
