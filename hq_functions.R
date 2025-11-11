# ----------------------------
# Hydro-Québec outages (R)
# ----------------------------
# install.packages(c("httr", "jsonlite", "dplyr", "tidyr", "stringr", "lubridate"))
# Optional for geometry: install.packages("sf")

library(httr)
library(jsonlite)
library(dplyr)
library(tidyr)
library(stringr)
library(lubridate)

suppressWarnings({
  has_sf <- requireNamespace("sf", quietly = TRUE)
})

BASE <- "https://pannes.hydroquebec.com/pannes/donnees/v3_0/"
TZ   <- "America/Toronto"

# ---------- Helpers ----------

# GET + parse JSON with retries
get_json <- function(url, retries = 3, timeout_sec = 20) {
  ua <- httr::user_agent("HQ-outages-R/1.0 (+https://example)")
  for (i in seq_len(retries)) {
    resp <- try(httr::GET(url, ua, httr::timeout(timeout_sec)), silent = TRUE)
    if (inherits(resp, "response") && httr::status_code(resp) == 200) {
      txt <- httr::content(resp, as = "text", encoding = "UTF-8")
      parsed <- try(jsonlite::fromJSON(txt, simplifyVector = FALSE), silent = TRUE)
      if (!inherits(parsed, "try-error")) return(parsed)
    }
    Sys.sleep(min(2^i, 5))
  }
  stop("Failed to fetch or parse JSON from: ", url)
}

# Extract 'version' regardless of shape (list, named/unnamed vector, bare string)
extract_version <- function(x) {
  if (is.null(x)) return(NA_character_)
  # common shape: list(version = "YYYYMMDDHHMMSS")
  v <- NULL
  if (is.list(x)) {
    v <- x$version %||na% x[["version"]]
    if (is.null(v) && length(x) >= 1) v <- x[[1]]
  } else if (is.atomic(x)) {
    nm <- names(x)
    if (!is.null(nm) && "version" %in% nm) v <- x[["version"]] else
      if (length(x) >= 1) v <- x[[1]]
  }
  v <- as.character(v)
  if (length(v) == 0) return(NA_character_)
  v[1]
}

# Null/empty coalesce (works with vectors/lists; doesn't assume length 1)
`%||na%` <- function(a, b) {
  if (is.null(a)) return(b)
  if (is.atomic(a) || is.list(a)) {
    if (length(a) == 0) return(b)
    return(a)
  }
  b
}

# Vectorized, robust datetime parser
parse_dt <- function(x, tz = TZ) {
  x <- as.character(x %||na% character())
  if (!length(x)) return(as.POSIXct(NA, tz = tz))
  
  # Pre-allocate result
  res <- rep(as.POSIXct(NA, tz = tz), length(x))
  
  # Numeric epoch (seconds or milliseconds)
  nx <- suppressWarnings(as.numeric(x))
  idx_epoch <- which(!is.na(nx))
  if (length(idx_epoch)) {
    ep <- nx[idx_epoch]
    # Heuristic: treat large values as milliseconds
    ep <- ifelse(ep > 1e12, ep / 1000, ep)
    res[idx_epoch] <- as.POSIXct(ep, origin = "1970-01-01", tz = tz)
  }
  
  # String dates
  parsed <- suppressWarnings(lubridate::parse_date_time(
    x,
    orders = c("ymd HMS", "ymd HM", "ymd",
               "Ymd HMS", "Ymd HM",
               "dmY HMS", "dmY HM"),
    tz = tz,
    quiet = TRUE
  ))
  fill_idx <- is.na(res) & !is.na(parsed)
  res[fill_idx] <- parsed[fill_idx]
  res
}

# Extract lon/lat from a string like "[lon, lat]"
parse_coords <- function(s) {
  s <- as.character(s)
  nums <- stringr::str_extract_all(s, "-?\\d+\\.?\\d*")
  lon <- lat <- rep(NA_real_, length(nums))
  for (i in seq_along(nums)) {
    if (length(nums[[i]]) >= 2) {
      lon[i] <- suppressWarnings(as.numeric(nums[[i]][1]))
      lat[i] <- suppressWarnings(as.numeric(nums[[i]][2]))
    }
  }
  list(lon = lon, lat = lat)
}

# ---------- Main: outages ----------

get_hq_outages <- function(return_sf = FALSE) {
  # 1) current version token
  ver_obj <- get_json(paste0(BASE, "bisversion.json"))
  ver <- extract_version(ver_obj)
  if (is.na(ver) || !nzchar(ver)) {
    stop("Couldn't read 'version' from bisversion.json. ",
         "Got: ", paste(capture.output(str(ver_obj)), collapse = " "))
  }
  
  # 2) outages payload
  raw <- get_json(paste0(BASE, "bismarkers", ver, ".json"))
  
  # Commonly the array is under 'pannes'; fall back to other plausible keys
  pannes <- raw$pannes %||na% raw$markers %||na% raw$outages
  if (is.null(pannes)) {
    # Sometimes top-level may directly be the list
    if (is.list(raw) && length(raw) > 0 && is.list(raw[[1]])) {
      pannes <- raw[[1]]
    }
  }
  
  if (is.null(pannes) || length(pannes) == 0) {
    message("No outages in payload.")
    return(tibble::tibble())
  }
  
  # Normalize rows (defensive against field count/order)
  df <- lapply(pannes, function(row) {
    row <- c(row, rep(NA, 16))[1:16]
    # coords typically at index 5
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

# ---------- Optional: polygons (affected areas) ----------

get_hq_polygons <- function() {
  ver <- extract_version(get_json(paste0(BASE, "bisversion.json")))
  if (is.na(ver) || !nzchar(ver)) stop("Couldn't read version for polygons.")
  kmz_url <- paste0(BASE, "bispoly", ver, ".kmz")
  
  tmp_kmz <- tempfile(fileext = ".kmz")
  tmp_dir <- tempfile()
  dir.create(tmp_dir, recursive = TRUE, showWarnings = FALSE)
  
  r <- httr::GET(kmz_url, httr::write_disk(tmp_kmz, overwrite = TRUE), httr::timeout(60))
  stopifnot(httr::status_code(r) == 200)
  
  # Try direct read; if it fails, unzip and read the KML
  if (has_sf) {
    direct <- try(sf::st_read(tmp_kmz, quiet = TRUE), silent = TRUE)
    if (inherits(direct, "sf")) return(direct)
  }
  
  utils::unzip(tmp_kmz, exdir = tmp_dir)
  kml <- list.files(tmp_dir, pattern = "\\.kml$", full.names = TRUE)[1]
  if (is.na(kml)) stop("No KML found inside KMZ.")
  if (!has_sf) stop("Package 'sf' not installed; cannot read KML.")
  sf::st_read(kml, quiet = TRUE)
}

# ---------- Example usage ----------

# Tabular tibble (no geometry)
outages <- get_hq_outages(FALSE)
print(outages |> dplyr::slice_head(n = 10))

# If you want POINT geometry:
# outages_sf <- get_hq_outages(TRUE)
# plot(sf::st_geometry(outages_sf))

# If you want polygons (KMZ/KML):
# polys_sf <- get_hq_polygons()
# polys_sf
# =========================
# Hydro-Québec polygons + joins (paste after your existing code)
# Requires: sf (and optionally leaflet)
# =========================

# Small helper to insist on packages (clear error if missing)
.hq_require_pkg <- function(pkg, why = "") {
  if (!requireNamespace(pkg, quietly = TRUE)) {
    stop(sprintf("Package '%s' is required%s. Install it with install.packages('%s')",
                 pkg, if (nzchar(why)) paste0(" for ", why) else "", pkg))
  }
}

# Clean polygons: set/transform CRS to WGS84, make valid, cast to MULTIPOLYGON
.hq_clean_polys <- function(x) {
  .hq_require_pkg("sf", "polygon cleaning")
  if (is.na(sf::st_crs(x))) sf::st_crs(x) <- 4326
  if (!identical(sf::st_crs(x)$epsg, 4326L)) x <- sf::st_transform(x, 4326)
  x <- suppressWarnings(sf::st_make_valid(x))
  gtypes <- sf::st_geometry_type(x, by_geometry = TRUE)
  if (all(grepl("POLYGON", toupper(gtypes)))) {
    x <- suppressWarnings(sf::st_cast(x, "MULTIPOLYGON"))
  }
  x
}

# ---- MAIN: fetch polygons for current live "version" ----
get_hq_polygons <- function(add_metadata = TRUE, quiet = TRUE) {
  .hq_require_pkg("sf", "polygons output")
  
  # 1) version
  ver_obj <- get_json(paste0(BASE, "bisversion.json"))
  ver <- extract_version(ver_obj)
  if (is.na(ver) || !nzchar(ver)) {
    stop("Couldn't read version for polygons. ",
         "Got: ", paste(capture.output(str(ver_obj)), collapse = " "))
  }
  
  # 2) download KMZ
  kmz_url <- paste0(BASE, "bispoly", ver, ".kmz")
  tmp_kmz <- tempfile(fileext = ".kmz")
  tmp_dir <- tempfile()
  dir.create(tmp_dir, recursive = TRUE, showWarnings = FALSE)
  
  r <- httr::GET(kmz_url, httr::write_disk(tmp_kmz, overwrite = TRUE), httr::timeout(60))
  if (httr::status_code(r) != 200) stop("Failed to download polygons: HTTP ", httr::status_code(r))
  
  # 3) Try direct KMZ read; if it fails (common), unzip -> read KML
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

# ---- JOIN: outages -> polygons, with safe summarise ----
hq_outages_join_polygons <- function(polys_sf = NULL, outages_sf = NULL) {
  .hq_require_pkg("sf", "spatial join")
  if (is.null(polys_sf))   polys_sf   <- get_hq_polygons()
  if (is.null(outages_sf)) outages_sf <- get_hq_outages(return_sf = TRUE)
  
  # Choose a polygon key column if one exists; else create 'poly_id'
  poly_id <- NULL
  for (cand in c("poly_id","id","ID","name","NOM","MUNICIPALITE")) {
    if (cand %in% names(polys_sf)) { poly_id <- cand; break }
  }
  if (is.null(poly_id)) {
    polys_sf$poly_id <- seq_len(nrow(polys_sf))
    poly_id <- "poly_id"
  }
  
  # Spatial join (points within polygons)
  joined <- sf::st_join(outages_sf, polys_sf, join = sf::st_within, left = FALSE)
  
  # Summaries per polygon
  joined_df <- sf::st_drop_geometry(joined)
  
  if ("customers" %in% names(joined_df)) {
    summary <- joined_df |>
      dplyr::group_by(.data[[poly_id]]) |>
      dplyr::summarise(
        n_outages    = dplyr::n(),
        customers_sum = sum(customers, na.rm = TRUE),
        .groups = "drop"
      )
  } else {
    summary <- joined_df |>
      dplyr::group_by(.data[[poly_id]]) |>
      dplyr::summarise(
        n_outages = dplyr::n(),
        .groups = "drop"
      )
  }
  
  # Join summary back to polygons using a stable key
  polys_aug <- polys_sf |>
    dplyr::mutate(poly_key = .data[[poly_id]]) |>
    dplyr::left_join(
      { tmp <- summary; names(tmp)[names(tmp) == poly_id] <- "poly_key"; tmp },
      by = "poly_key"
    ) |>
    dplyr::select(-poly_key)
  
  list(
    polygons      = polys_aug,
    joined_points = joined,
    summary       = summary
  )
}

# ---- Quick interactive map (optional; requires leaflet) ----
hq_leaflet_map <- function(polys_sf = NULL, outages_sf = NULL,
                           polygon_weight = 1, polygon_opacity = 0.25, point_radius = 5) {
  .hq_require_pkg("leaflet", "interactive map")
  if (is.null(polys_sf))   polys_sf   <- get_hq_polygons()
  if (is.null(outages_sf)) outages_sf <- get_hq_outages(return_sf = TRUE)
  
  # Simple popups
  poly_popup_fields <- intersect(c("hq_version","hq_retrieved_at","name","NOM","MUNICIPALITE","poly_id","id","ID"),
                                 names(polys_sf))
  poly_popup <- if (length(poly_popup_fields)) {
    apply(as.data.frame(polys_sf[poly_popup_fields], stringsAsFactors = FALSE), 1, function(r) {
      paste(paste0("<b>", names(r), ":</b> ", r), collapse = "<br/>")
    })
  } else NULL
  
  pt_fields <- intersect(c("customers","status","cause_group","started_at","eta_at"), names(outages_sf))
  pt_popup <- if (length(pt_fields)) {
    apply(as.data.frame(sf::st_drop_geometry(outages_sf)[pt_fields], stringsAsFactors = FALSE), 1, function(r) {
      paste(paste0("<b>", names(r), ":</b> ", r), collapse = "<br/>")
    })
  } else NULL
  
  leaflet::leaflet() |>
    leaflet::addTiles() |>
    leaflet::addPolygons(data = polys_sf,
                         weight = polygon_weight,
                         fillOpacity = polygon_opacity,
                         popup = poly_popup) |>
    leaflet::addCircleMarkers(data = outages_sf,
                              radius = point_radius,
                              stroke = FALSE,
                              fillOpacity = 0.9,
                              popup = pt_popup)
}

# ---- Examples ----
polys <- get_hq_polygons()
res   <- hq_outages_join_polygons(polys)
res$summary |> dplyr::arrange(dplyr::desc(n_outages)) |> head()
hq_leaflet_map(res$polygons, res$joined_points)
